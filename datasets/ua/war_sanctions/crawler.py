from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_html


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
    {  # uav manufacturers
        "url": "https://war-sanctions.gur.gov.ua/en/uav/companies",
        "type": "legal_entity",
        "program": "Legal entities involved in the production of UAVs",
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
    {  # captains
        "url": "https://war-sanctions.gur.gov.ua/en/transport/captains",
        "type": "captain",
        "program": "Captains of ships involved in the transportation of weapons, stolen Ukrainian products and in the circumvention of sanctions",
    },
    {  # propagandists
        "url": "https://war-sanctions.gur.gov.ua/en/propaganda/persons?page=1&per-page=12",
        "type": "person",
        "program": "Persons involved in the dissemination of propaganda",
    },
    {  # executives of war
        "url": "https://war-sanctions.gur.gov.ua/en/executives",
        "type": "person",
        "program": "Officials and entities controlling Russia’s military-industrial policy, defense orders, and wartime economy",
    },
]


def extract_label_value_pair(label_elem, value_elem, data):
    label = label_elem.text_content().strip().replace("\n", " ")
    value = [text.strip() for text in value_elem.itertext() if text.strip()]
    if len(value) == 1:
        value = value[0]
    data[label] = value
    return label, value


def apply_dob_pob(entity, dob_pob):
    if not dob_pob:
        return
    # If we get more than one part, unpack it into dob and pob
    if len(dob_pob) == 2:
        dob, pob = dob_pob
        h.apply_date(entity, "birthDate", dob)
        entity.add("birthPlace", pob)
    # If there’s only one part, we assume it's just the dob
    elif len(dob_pob) == 1:
        dob = dob_pob[0]
        h.apply_date(entity, "birthDate", dob)


def emit_care_of(context, entity, unknown_link_name):
    # create an unknown link entity fo c/o cases in names
    care_of_entity = context.make("LegalEntity")
    care_of_entity.id = context.make_id(unknown_link_name)
    care_of_entity.add("name", unknown_link_name)
    context.emit(care_of_entity)

    # create and emit the UnknownLink
    unknown_link = context.make("UnknownLink")
    unknown_link.id = context.make_id(entity.id, "unknown_link", care_of_entity.id)
    unknown_link.add("object", entity.id)
    unknown_link.add("subject", care_of_entity.id)
    unknown_link.add("role", "c/o")
    context.emit(unknown_link)


def crawl_index_page(context: Context, index_page, data_type, program):
    index_page = fetch_html(
        context,
        index_page,
        unblock_validator=".//div[@id='main-grid']",
        html_source="httpResponseBody",
    )
    main_grid = index_page.find('.//div[@id="main-grid"]')
    if data_type == "captain":
        crawl_captain(context, main_grid, program)
    for link in main_grid.xpath(".//a/@href"):
        if link.startswith("https:"):
            if data_type == "person":
                crawl_person(context, link, program)
            elif data_type == "legal_entity":
                crawl_legal_entity(context, link, program)
            if data_type == "vessel":
                crawl_vessel(context, link, program)


def crawl_captain(context: Context, main_grid, program):
    captain_container = main_grid.xpath(
        ".//div[contains(@class, 'component-link item-cell')]"
    )
    data: dict[str, str] = {}
    for captain in captain_container:
        for row in captain.xpath(".//div[@class='row']"):
            divs = row.findall("div")
            if len(divs) == 2:
                label_elem, value_elem = divs
                if "yellow" in value_elem.get("class"):
                    label, value = extract_label_value_pair(
                        label_elem, value_elem, data
                    )
                    link_elem = value_elem.find(".//a[@href]")
                    if link_elem is not None:
                        vessel_url = link_elem.get("href")
                    data[label] = value

        name = data.pop("Name")
        dob_pob = data.pop("Date and place of birth")
        tax_number = data.pop("Tax Number")
        vessel_name = data.pop("Captain of the vessel")
        vessel_category = data.pop("Category of the vessel")

        captain = context.make("Person")
        captain.id = context.make_id(name, dob_pob, tax_number)
        captain.add("name", name)
        captain.add("taxNumber", tax_number)
        captain.add("topics", "poi")
        if dob_pob:
            apply_dob_pob(captain, dob_pob)

        sanction = h.make_sanction(context, captain)
        sanction.add("program", program)

        context.emit(captain)
        context.emit(sanction)

        vessel = context.make("Vessel")
        vessel.id = context.make_id(vessel_name, vessel_category, vessel_url)
        vessel.add("name", vessel_name)
        vessel.add("sourceUrl", vessel_url)
        vessel.add("notes", vessel_category)

        link = context.make("UnknownLink")
        link.id = context.make_id(captain.id, "captain", vessel.id)
        link.add("subject", captain.id)
        link.add("object", vessel.id)
        link.add("role", "captain")

        context.emit(vessel)
        context.emit(link)


def crawl_vessel(context: Context, link, program):
    detail_page = fetch_html(
        context,
        link,
        unblock_validator=".//div[contains(@class,'tools-spec')]/div[contains(@class, 'row')]",
        html_source="httpResponseBody",
        cache_days=1,
    )
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
    vessel.add("description", data.pop("Vessel information"))
    vessel.add("callSign", data.pop("Call sign"))
    vessel.add("flag", data.pop("Flag (Current)"))
    vessel.add("mmsi", data.pop("MMSI"))
    vessel.add("buildDate", data.pop("Build year"))
    for raw_link in web_links:
        link_href = raw_link.get("href", "").strip()
        vessel.add("sourceUrl", link_href)
    vessel.add("notes", data.pop("Category"))
    for pr_name in h.multi_split(data.pop("Former ship names"), [" / "]):
        vessel.add("previousName", pr_name)
    for past_flag in h.multi_split(data.pop("Flags (former)"), [" /"]):
        vessel.add("pastFlags", past_flag)
    vessel.add("topics", "poi")
    vessel.add("sourceUrl", link)

    sanction = h.make_sanction(context, vessel)
    sanction.add("program", program)
    sanction.add("sourceUrl", link)

    context.emit(vessel)
    context.emit(sanction)

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
        data.pop("Ship Owner (IMO / Country / Date)"),
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
            # These always seem to be one of the owner or management companies
            # already included from that section.
            "The person in connection with whom sanctions have been applied",
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
        entity_name_number, entity_country, entity_date = relation_parts
        if len(h.multi_split(entity_name_number, [" (", "c/o"])) == 2:
            entity_name, registration_number = entity_name_number.split(" (")
            care_of = None
        else:
            override_res = context.lookup("overrides", entity_name_number)
            if override_res:
                entity_name = override_res.name
                registration_number = override_res.registration_number
                care_of = override_res.care_of
            else:
                context.log.warning(
                    f'No override found for "{entity_name_number}".',
                    key=entity_name_number,
                )
                return

        entity = context.make("Organization")
        entity.id = context.make_id(entity_name, entity_country)
        entity.add("name", entity_name)
        entity.add("imoNumber", registration_number)
        entity.add("country", entity_country)
        context.emit(entity)

        relation = context.make(rel_schema)
        relation.id = context.make_id(vessel.id, rel_role, entity.id)
        relation.add(from_prop, vessel.id)
        relation.add(to_prop, entity.id)
        relation.add("role", rel_role)
        h.apply_date(relation, "startDate", entity_date)
        context.emit(relation)

        if care_of is not None:
            emit_care_of(context, entity, care_of)


def crawl_person(context: Context, link, program):
    detail_page = fetch_html(
        context,
        link,
        unblock_validator=".//main//div[@class='row']",
        html_source="httpResponseBody",
        cache_days=1,
    )

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
    dob_pob = data.pop("Date and place of birth", None)
    archive_links = data.pop("Archive links", None)

    person = context.make("Person")
    person.id = context.make_id(names, positions)
    for name in h.multi_split(names, [" | "]):
        person.add("name", name)
    person.add("citizenship", data.pop("Citizenship", None))
    person.add("taxNumber", data.pop("Tax Number", None))
    person.add("sourceUrl", data.pop("Links", None))
    person.add("position", data.pop("Other positions", None))
    h.apply_date(person, "birthDate", data.pop("Date of birth", None))
    person.add(
        "position",
        data.pop(
            "Positions or membership in the governance bodies of the russian MIC", None
        ),
    )
    person.add("topics", "poi")
    if archive_links is not None:
        person.add("sourceUrl", archive_links)
    if dob_pob:
        apply_dob_pob(person, dob_pob)
    if positions:
        for position in h.multi_split(positions, [" / "]):
            person.add("position", position)

    sanction = h.make_sanction(context, person)
    sanction.add("reason", data.pop("Reasons", None))
    sanction.add("sourceUrl", link)
    sanction.add("program", program)

    context.emit(person)
    context.emit(sanction)
    context.audit_data(data, ignore=["Sanction Jurisdictions"])


def crawl_legal_entity(context: Context, link, program):
    detail_page = fetch_html(
        context,
        link,
        unblock_validator=".//main//div[@class='row']",
        html_source="httpResponseBody",
        cache_days=1,
    )

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
    name = data.pop("Name", None)
    if name is None:
        name = data.pop("Full name of legal entity")
    name_abbr = data.pop("Abbreviated name of the legal entity", None)
    reg_num = data.pop("Registration number")

    legal_entity = context.make("LegalEntity")
    legal_entity.id = context.make_id(name, name_abbr, reg_num)
    legal_entity.add("name", name)
    legal_entity.add("name", name_abbr)
    legal_entity.add("ogrnCode", reg_num)
    legal_entity.add("address", data.pop("Address"))
    legal_entity.add("country", data.pop("Country"))
    legal_entity.add("innCode", data.pop("Tax Number"))
    legal_entity.add("sourceUrl", data.pop("Links", None))
    archive_links = data.pop("Archive links", None)
    if archive_links is not None:
        legal_entity.add("sourceUrl", archive_links)

    legal_entity.add("topics", "poi")
    sanction = h.make_sanction(context, legal_entity)
    sanction.add("reason", data.pop("Reasons", None))
    sanction.add("sourceUrl", link)
    sanction.add("program", program)

    context.emit(legal_entity)
    context.emit(sanction)
    context.audit_data(data, ignore=["Sanction Jurisdictions", "Products"])


def extract_next_page_url(doc):
    # next page <a> element extraction using xpath
    next_link_element = doc.xpath("//ul[@class='pagination']//li[@class='next']/a")
    if next_link_element:
        next_link = next_link_element[0]
        return next_link.get("href")

    return None


def crawl(context: Context):
    main_page = fetch_html(
        context,
        context.data_url,
        unblock_validator=".//section[contains(@class, 'sections')][contains(@class, 'justify-content-center')]",
        html_source="httpResponseBody",
        cache_days=1,
    )
    # Have any new sections been added?
    section_links_section = main_page.xpath(
        ".//section[contains(@class, 'sections')][contains(@class, 'justify-content-center')]"
    )
    assert len(section_links_section) == 1, section_links_section
    h.assert_dom_hash(
        section_links_section[0], "b66069bcdb6a9a977a668210ddaddb398998f1b8"
    )

    # Has the API link been updated to point to the previously-nonexistent API page?
    api_link = main_page.xpath(".//div//span[contains(text(), 'API')]/ancestor::div[1]")
    assert len(api_link) == 1, api_link
    h.assert_dom_hash(api_link[0], "a11857d8bb4774630bc85a4b8f2563df141c8cc1")

    # Has anything been added to the transport tabs?
    transport_page = fetch_html(
        context,
        "https://war-sanctions.gur.gov.ua/en/transport/ships",
        unblock_validator=".//div[@id='main-grid']",
        html_source="httpResponseBody",
        cache_days=1,
    )
    transport_tabs_container = transport_page.xpath(
        ".//div[contains(@class, 'tab')]/div[contains(@class, 'justify-content-center')]"
    )
    assert len(transport_tabs_container) == 1, transport_tabs_container
    h.assert_dom_hash(
        transport_tabs_container[0], "30a19544db4cf42e1c8678f243974e9d12dfa6aa"
    )

    for link_info in LINKS:
        base_url = link_info["url"]
        data_type = link_info["type"]
        program = link_info["program"]
        current_url = base_url
        visited_pages = 0
        while current_url:
            doc = fetch_html(
                context,
                current_url,
                unblock_validator=".//div[@id='main-grid']",
                html_source="httpResponseBody",
            )
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
