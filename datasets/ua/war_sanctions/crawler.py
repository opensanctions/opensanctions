from zavod import Context, helpers as h


LINKS = [
    {  # child kidnappers
        "url": "https://war-sanctions.gur.gov.ua/en/kidnappers/persons?page=1&per-page=12",
        "type": "person",
        "program": "Persons involved in the deportation of Ukrainian children",
    },
    {  # child kidnappers
        "url": "https://war-sanctions.gur.gov.ua/en/kidnappers/companies?page=1&per-page=12",
        "type": "legal_entity",
        "program": "Legal entities involved in the deportation of Ukrainian children",
    },
    {  # russian athletes
        "url": "https://war-sanctions.gur.gov.ua/en/sport/persons?page=1&per-page=12",
        "type": "person",
        "program": "Athletes and sports officials participating in Russian influence operations abroad",
    },
    {  # ships
        "url": "https://war-sanctions.gur.gov.ua/en/transport/ships?page=1&per-page=12",
        "type": "vessel",
        "program": "Marine and Aircraft Vessels, Airports and Ports involved in the transportation of weapons, stolen Ukrainian products and in the circumvention of sanctions",
    },
]


def lookup_override(context, key):
    override_res = context.lookup("overrides", key)
    if not override_res:
        context.log.warning(f"No override found for {key}")
        return {}

    extracted = {}
    for override in override_res.items:
        extracted[override["prop"]] = override["value"]

    if not extracted:
        context.log.warning(f"No matching properties found in overrides for {key}")

    return extracted


def extract_label_value_pair(label_elem, value_elem, data):
    label = label_elem.text_content().strip().replace("\n", " ")
    value = [text.strip() for text in value_elem.itertext() if text.strip()]
    if len(value) == 1:
        value = value[0]
    data[label] = value
    return label, value


def crawl_index_page(context: Context, index_page, data_type, program):
    index_page = context.fetch_html(index_page)
    main_grid = index_page.find('.//div[@id="main-grid"]')
    for link in main_grid.xpath(".//a/@href"):
        if link.startswith("https:"):
            if data_type == "person":
                crawl_person(context, link, program)
            elif data_type == "legal_entity":
                crawl_legal_entity(context, link, program)
            if data_type == "vessel":
                crawl_vessel(context, link, program)


def crawl_vessel(context: Context, link, program):
    detail_page = context.fetch_html(link, cache_days=1)
    details_container = detail_page.find(".//main")
    data: dict[str, str] = {}

    # pop() without defaults below imply validity of the very generic selectors,
    # and audit() validates the exclusion of irrelevant data.
    xpath_definitions = [
        # e.g. Vessel name, IMO, Call sign
        (
            "main_info_rows",
            ".//div[contains(@class,'tools-spec')]/div[contains(@class, 'row')]",
        ),
        # e.g. `Vessel information`
        (
            "justification_rows",
            ".//div[contains(@class, 'tools-frame')]/div[contains(@class, 'mb-3')]",
        ),
        # e.g. Shipowner, Ship Safety Management Manager, Former ship names...
        (
            "additional_info_rows",
            ".//div[contains(@class, 'tools-frame')]//div[@class='mb-3' or contains(@class, 'js_visibility')]",
        ),
    ]
    for name, xpath_expr in xpath_definitions:
        rows = details_container.xpath(xpath_expr)
        for row in rows:
            divs = row.findall("div")
            if len(divs) == 2:
                label_elem, value_elem = divs
                if "yellow" in value_elem.get("class"):
                    label, value = extract_label_value_pair(
                        label_elem, value_elem, data
                    )
                    data[label] = value

    web_links = details_container.xpath(
        ".//div[contains(@class, 'tools-frame')]//a[contains(@class, 'long-text yellow')]"
    )

    name = data.pop("Vessel name (international according to IMO)")
    type = data.pop("Vessel Type")
    imo_num = data.pop("IMO")

    vessel = context.make("Vessel")
    vessel.id = context.make_id(name, imo_num)
    vessel.add("name", name)
    vessel.add("imoNumber", imo_num)
    vessel.add("type", type)

    # pop() without defaults here imply validity of the very generic selectors
    # for these attributes.
    vessel.add("description", " ".join(data.pop("Vessel information")))
    vessel.add("callSign", data.pop("Call sign"))
    vessel.add("flag", data.pop("Flag (Current)"))
    vessel.add("mmsi", data.pop("MMSI"))
    vessel.add("buildDate", data.pop("Build year"))
    for raw_link in web_links:
        link_href = raw_link.get("href", "").strip()
        vessel.add("sourceUrl", link_href)
    vessel.add("keywords", data.pop("Category"))
    for pr_name in h.multi_split(data.pop("Former ship names"), [" / "]):
        vessel.add("previousName", pr_name)
    for past_flag in h.multi_split(data.pop("Flags (former)"), [" /"]):
        vessel.add("pastFlags", past_flag)
    vessel.add("topics", "poi")

    sanction = h.make_sanction(context, vessel)
    sanction.add("program", program)
    sanction.add("sourceUrl", link)

    context.emit(vessel, target=True)
    context.emit(sanction)

    linked_entity_name = data.pop(
        "The person in connection with whom sanctions have been applied"
    )
    if linked_entity_name != "":
        linked_entity = context.make("LegalEntity")
        linked_entity.id = context.make_id(linked_entity_name, "uknown_link")
        linked_entity.add("name", linked_entity_name)
        linked_entity.add("topics", "poi")
        context.emit(linked_entity, target=True)

        unknown_link = context.make("UnknownLink")
        unknown_link.id = context.make_id(vessel.id, "uknown_link", linked_entity.id)
        unknown_link.add("object", vessel.id)
        unknown_link.add("subject", linked_entity.id)
        unknown_link.add(
            "role", "The entity in connection with which sanctions have been applied"
        )
        context.emit(unknown_link)

    crawl_ship_relation(
        context,
        vessel,
        data.pop("Commercial ship manager (IMO / Country / Date)"),
        "Commercial ship manager",
        "UnknownLink",
        "subject",
        "object",
    )
    crawl_ship_relation(
        context,
        vessel,
        data.pop("Ship Safety Management Manager (IMO / Country / Date)", None),
        "Ship Safety Management Manager",
        "UnknownLink",
        "subject",
        "object",
    )
    crawl_ship_relation(
        context,
        vessel,
        data.pop("Shipowner (IMO / Country / Date)"),
        "Shipowner",
        "Ownership",
        "asset",
        "owner",
    )
    context.audit_data(
        data,
        ignore=[
            "Sanctions",
            "Cases of AIS shutdown",
            "Calling at russian ports",
            "Visited ports",
            "Builder (country)",
        ],
    )


def crawl_ship_relation(
    context,
    vessel,
    relation_info,
    rel_role,
    rel_schema,
    from_prop,
    to_prop,
):
    if relation_info is None:
        return
    # Split the relation info into expected parts
    relation_parts = relation_info.split(" / ")
    if len(relation_parts) == 3:
        entity_name_imo, entity_country, entity_date = relation_parts
        if len(h.multi_split(entity_name_imo, [" (", "c/o"])) != 2:
            overrides = lookup_override(context, entity_name_imo)
            entity_name = overrides.get("name")
            entity_imo = overrides.get("registrationCode")
        else:
            entity_name, entity_imo = entity_name_imo.split(" (")

        # Create and emit the Legal Entity
        entity = context.make("LegalEntity")
        entity.id = context.make_id(entity_name, entity_country)
        entity.add("name", entity_name)
        entity.add("registrationNumber", entity_imo)
        entity.add("country", entity_country)
        entity.add("topics", "poi")
        context.emit(entity, target=True)

        # Create the relation representation
        relation = context.make(rel_schema)
        relation.id = context.make_id(vessel.id, rel_role, entity.id)

        # Set the appropriate field based on the role
        relation.add(from_prop, vessel.id)
        relation.add(to_prop, entity.id)
        relation.add("role", rel_role)

        h.apply_date(relation, "startDate", entity_date)
        context.emit(relation)


def crawl_person(context: Context, link, program):
    detail_page = context.fetch_html(link, cache_days=1)

    # Having at least some pop()s without defaults and audit()-ing the rest
    # implies the very generic selectors.
    data: dict[str, str] = {}
    for row in detail_page.findall(".//main//div[@class='row']"):
        divs = row.findall("div")
        if len(divs) == 2:
            label_elem, value_elem = divs
            if "yellow" in label_elem.get("class"):
                label, value = extract_label_value_pair(label_elem, value_elem, data)
                data[label] = value
    names = data.pop("Name")
    positions = data.pop("Position", None)

    person = context.make("Person")
    person.id = context.make_id(names, positions)
    for name in h.multi_split(names, [" | "]):
        person.add("name", name)
    person.add("citizenship", data.pop("Citizenship", None))
    person.add("taxNumber", data.pop("Tax Number", None))
    person.add("sourceUrl", data.pop("Links"))
    archive_links = data.pop("Archive links", None)
    if archive_links is not None:
        person.add("sourceUrl", archive_links)
    dob_pob = data.pop("Date and place of birth", None)
    if dob_pob:
        # If we get more than one part, unpack it into dob and pob
        if len(dob_pob) == 2:
            dob, pob = dob_pob
            h.apply_date(person, "birthDate", dob)
            person.add("birthPlace", pob)
        elif len(dob_pob) == 1:
            # If there’s only one part, we assume it's just the dob
            dob = dob_pob[0]
            h.apply_date(person, "birthDate", dob)
    if positions:
        for position in h.multi_split(positions, [" / "]):
            person.add("position", position)
    person.add("topics", "poi")

    sanction = h.make_sanction(context, person)
    sanction.add("reason", " ".join(data.pop("Reasons")))
    sanction.add("sourceUrl", link)
    sanction.add("program", program)

    context.emit(person, target=True)
    context.emit(sanction)
    context.audit_data(data, ignore=["Sanction Jurisdictions"])


def crawl_legal_entity(context: Context, link, program):
    detail_page = context.fetch_html(link, cache_days=1)

    # Having at least some pop()s without defaults and audit()-ing the rest
    # implies the very generic selectors.
    data = {}
    for row in detail_page.findall(".//main//div[@class='row']"):
        divs = row.findall("div")
        if len(divs) == 2:
            label_elem, value_elem = divs
            if "yellow" in label_elem.get("class"):
                label, value = extract_label_value_pair(label_elem, value_elem, data)
                data[label] = value
    name = data.pop("Name")
    name_abbr = data.pop("Abbreviated name of the legal entity", None)
    reg_num = data.pop("Registration number")

    legal_entity = context.make("LegalEntity")
    legal_entity.id = context.make_id(name, name_abbr, reg_num)
    legal_entity.add("name", name)
    legal_entity.add("name", name_abbr)
    legal_entity.add("registrationNumber", reg_num)
    legal_entity.add("address", data.pop("Address"))
    legal_entity.add("country", data.pop("Country"))
    legal_entity.add("taxNumber", data.pop("Tax Number"))
    legal_entity.add("sourceUrl", data.pop("Links"))
    archive_links = data.pop("Archive links", None)
    if archive_links is not None:
        legal_entity.add("sourceUrl", archive_links)

    legal_entity.add("topics", "poi")
    sanction = h.make_sanction(context, legal_entity)
    sanction.add("reason", " ".join(data.pop("Reasons")))
    sanction.add("sourceUrl", link)
    sanction.add("program", program)

    context.emit(legal_entity, target=True)
    context.emit(sanction)
    context.audit_data(data, ignore=["Sanction Jurisdictions"])


def extract_next_page_url(doc):
    # next page <a> element extraction using xpath
    next_link_element = doc.xpath("//ul[@class='pagination']//li[@class='next']/a")
    if next_link_element:
        next_link = next_link_element[0]
        return next_link.get("href")

    return None


def crawl(context: Context):
    main_page = context.fetch_html(context.data_url)
    node = main_page.find(
        ".//section[@class='sections d-flex flex-wrap align-items-stretch justify-content-center mb-5 pl-5 pr-5 medium']"
    )
    api_section = main_page.find(
        ".//div[@class='d-flex flex-column flex-sm-row menu-alt']"
    )
    h.assert_dom_hash(node, "dbb9a924d940b3a69a132a102e04dcf0f9fbfc5e")
    h.assert_dom_hash(api_section, "78bcb227e62e598db7e53e56db0d25ac70051817")
    h.assert_html_url_hash(
        context,
        "https://war-sanctions.gur.gov.ua/en/transport/ships",
        "040d13069019b10c8a7613ed3fc0c71ca5255e66",
        path=".//div[@class='tab wide active yellow d-flex justify-content-center gap30']",
    )

    for link_info in LINKS:
        base_url = link_info["url"]
        data_type = link_info["type"]
        program = link_info["program"]
        current_url = base_url
        visited_pages = 0
        while current_url:
            doc = context.fetch_html(current_url)
            doc.make_links_absolute(base_url)
            if doc is None:
                context.log.warn(f"Failed to fetch {current_url}")
                break
            context.log.info(f"Processing {current_url}")
            crawl_index_page(context, current_url, data_type, program)

            # get the next page URL, if exists
            next_url = extract_next_page_url(doc)
            current_url = next_url
            visited_pages += 1

            if visited_pages >= 100:
                raise Exception(
                    "Emergency limit of 100 visited pages reached. Potential logical inconsistency detected."
                )
