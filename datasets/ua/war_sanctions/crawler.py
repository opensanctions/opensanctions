from zavod import Context, helpers as h


LINKS = [
    {  # child kidnappers
        "url": "https://war-sanctions.gur.gov.ua/en/kidnappers/persons?page=1&per-page=12",
        "type": "person",
    },
    {  # child kidnappers
        "url": "https://war-sanctions.gur.gov.ua/en/kidnappers/companies?page=1&per-page=12",
        "type": "company",
    },
    {  # russian athletes
        "url": "https://war-sanctions.gur.gov.ua/en/sport/persons?page=1&per-page=12",
        "type": "person",
    },
    {  # ships
        "url": "https://war-sanctions.gur.gov.ua/en/transport/ships?page=1&per-page=12",
        "type": "vessel",
    },
]

# TODO: c/o and uknowns


def lookup_override(context, key, lookup_type):
    override_res = context.lookup("overrides", key)
    if not override_res:
        context.log.warning(f"No override found for {key}")
        return {}

    extracted = {}
    for override in override_res.items:
        if override["prop"] == "name":
            extracted["name"] = override["value"]
        elif override["prop"] == "registrationCode":
            extracted["registrationCode"] = override["value"]

    if not extracted:
        context.log.warning(f"No matching properties found in overrides for {key}")

    return extracted


def extract_label_value_pair(label_elem, value_elem, data):
    label = label_elem.text_content().strip().replace("\n", " ")
    value = value_elem.text_content().strip().replace("\n", " ")
    value = " ".join(value.split())
    # we use it for splitting some strings at a later stage
    value = " | ".join(
        [text.strip() for text in value_elem.itertext() if text.strip()]
    ).strip()
    data[label] = value

    return data


def crawl_index_page(context: Context, index_page, data_type):
    index_page = context.fetch_html(index_page, cache_days=3)
    main_grid = index_page.find('.//div[@id="main-grid"]')
    if main_grid is not None:
        for a in main_grid.findall(".//a"):
            href = [a.get("href")]
            for link in href:
                if link.startswith("https:"):
                    detail_page = context.fetch_html(link, cache_days=3)
                    if data_type == "person":
                        details_container = detail_page.find(
                            ".//div[@id='js_visibility'][@class='col-12 col-lg-9']"
                        )
                        crawl_person(context, details_container, link)
                    elif data_type == "company":
                        details_container = detail_page.find(
                            ".//div[@class='col-12 col-lg-9']"
                        )
                        crawl_company(context, details_container, link)
                    if data_type == "vessel":
                        details_container = detail_page.find(
                            ".//div[@id='js_visibility']"
                        )
                        crawl_vessel(context, details_container, link)


def crawl_vessel(context: Context, details_container, link):
    data: dict[str, str] = {}

    xpath_definitions = [
        (
            "main_info_rows",
            ".//div[contains(@class,'tools-spec')]/div[contains(@class, 'row')]",
        ),
        (
            "justification_rows",
            ".//div[contains(@class, 'tools-frame')]/div[contains(@class, 'mb-3')]",
        ),
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
                    data = extract_label_value_pair(label_elem, value_elem, data)

    web_resources = []
    web_links = details_container.xpath(
        ".//div[contains(@class, 'tools-frame')]//a[contains(@class, 'long-text yellow')]"
    )
    for raw_link in web_links:
        link_href = raw_link.get("href", "").strip()
        web_resources.append(link_href)
    data["Web resources"] = web_resources

    name = data.pop("Vessel name (international according to IMO)")
    type = data.pop("Vessel Type")
    imo_num = data.pop("IMO")
    categories = data.pop("Category")
    flags_former = data.pop("Flags (former)")

    vessel = context.make("Vessel")
    vessel.id = context.make_id(name, imo_num)
    vessel.add("name", name)
    vessel.add("imoNumber", imo_num)
    vessel.add("type", type)
    vessel.add("description", data.pop("Vessel information").replace(" | ", " "))
    vessel.add("callSign", data.pop("Call sign"))
    vessel.add("flag", data.pop("Flag (Current)"))
    vessel.add("mmsi", data.pop("MMSI"))
    vessel.add("buildDate", data.pop("Build year"))
    for category in categories.split(" | "):
        vessel.add("keywords", category)
    for web_resource in data.pop("Web resources"):
        vessel.add("sourceUrl", web_resource)
    for name in h.multi_split(data.pop("Former ship names"), [" / "]):
        vessel.add("previousName", name)
    for flag in flags_former.split(" / |"):
        vessel.add("pastFlags", flag)
    vessel.add("topics", "poi")

    sanction = h.make_sanction(context, vessel)
    sanction.add("country", data.pop("Sanctions", None))
    sanction.add("sourceUrl", link)

    context.emit(vessel, target=True)
    context.emit(sanction)

    linked_entity_name = data.pop(
        "The person in connection with whom sanctions have been applied"
    )
    if linked_entity_name != "":
        linked_entity = context.make("LegalEntity")
        linked_entity.id = context.make_id(linked_entity_name)
        for name in h.multi_split(linked_entity_name, [" | "]):
            linked_entity.add("name", name)
        linked_entity.add("topics", "poi")
        context.emit(linked_entity, target=True)

    com_manager = data.pop("Commercial ship manager (IMO / Country / Date)")
    if com_manager != "":
        com_manager_parts = com_manager.split(" / ")
        if len(com_manager_parts) == 3:
            com_name_imo, com_country, com_date = com_manager_parts
            # we do not expect to actually split on 'c/o' in any of this cases
            # it's there only to trigger lookup_override
            if len(h.multi_split(com_name_imo, [" (", "c/o"])) == 2:
                com_name, com_imo = com_name_imo.split(" (")
            else:
                overrides = lookup_override(context, com_name_imo, "com_manager")
                com_name = overrides.get("name")
                com_imo = overrides.get("registrationCode")

            com_manager = context.make("LegalEntity")
            com_manager.id = context.make_id(com_name, com_country)
            com_manager.add("name", com_name)
            com_manager.add("registrationNumber", com_imo)
            com_manager.add("country", com_country)
            com_manager.add("topics", "poi")
            context.emit(com_manager, target=True)

            com_rep = context.make("Representation")
            com_rep.id = context.make_id(
                vessel.id, "commercially managed by", com_manager.id
            )
            com_rep.add("client", vessel.id)
            com_rep.add("agent", com_manager.id)
            com_rep.add("role", "Commercial ship manager")
            h.apply_date(com_rep, "startDate", com_date)

            context.emit(com_rep)

    safety_manager = data.pop(
        "Ship Safety Management Manager (IMO / Country / Date)", None
    )
    if safety_manager is not None:
        safety_manager_parts = safety_manager.split(" / ")
        if len(safety_manager_parts) == 3:
            safety_name_imo, safety_country, safety_date = safety_manager_parts
            if len(h.multi_split(safety_name_imo, [" (", "c/o"])) == 2:
                safety_name, safety_imo = safety_name_imo.split(" (")
            else:
                overrides = lookup_override(context, safety_name_imo, "com_manager")
                safety_name = overrides.get("name")
                safety_imo = overrides.get("registrationCode")

            safety_manager = context.make("LegalEntity")
            safety_manager.id = context.make_id(safety_name, safety_country)
            safety_manager.add("name", safety_name)
            safety_manager.add("registrationNumber", safety_imo)
            safety_manager.add("country", safety_country)
            safety_manager.add("topics", "poi")
            context.emit(safety_manager, target=True)

            safety_rep = context.make("Representation")
            safety_rep.id = context.make_id(
                vessel.id, "safety managed by", safety_manager.id
            )
            safety_rep.add("client", vessel.id)
            safety_rep.add("agent", safety_manager.id)
            safety_rep.add("role", "Ship Safety Management Manager")
            h.apply_date(safety_rep, "startDate", safety_date)

            context.emit(safety_rep)

    owner_info = data.pop("Shipowner (IMO / Country / Date)")
    if owner_info != "":
        owner_parts = owner_info.split(" / ")
        if len(owner_parts) == 3:
            owner_name_imo, owner_country, owner_date = owner_parts
            if len(h.multi_split(owner_name_imo, [" (", "c/o"])) == 2:
                owner_name, owner_imo = owner_name_imo.split(" (")
            else:
                overrides = lookup_override(context, owner_name_imo, "com_manager")
                owner_name = overrides.get("name")
                owner_imo = overrides.get("registrationCode")

            owner = context.make("LegalEntity")
            owner.id = context.make_id(owner_name, owner_country)
            owner.add("name", owner_name)
            owner.add("country", owner_country)
            owner.add("registrationNumber", owner_imo)
            owner.add("topics", "poi")
            context.emit(owner, target=True)

            ownership = context.make("Ownership")
            ownership.id = context.make_id(vessel.id, "owned by", owner.id)
            ownership.add("asset", vessel.id)
            ownership.add("owner", owner.id)
            ownership.add("ownershipType", "Owner")
            h.apply_date(ownership, "startDate", owner_date)

            context.emit(ownership)

    context.audit_data(
        data,
        ignore=[
            "Cases of AIS shutdown",
            "Calling at russian ports",
            "Visited ports",
            "Builder (country)",
        ],
    )


def crawl_person(context: Context, details_container, link):
    data: dict[str, str] = {}
    for row in details_container.findall(".//div[@class='row']"):
        divs = row.findall("div")
        if len(divs) == 2:
            label_elem, value_elem = divs
            if "yellow" in label_elem.get("class"):
                data = extract_label_value_pair(label_elem, value_elem, data)

    names = data.pop("Name")
    positions = data.pop("Position", None)

    person = context.make("Person")
    person.id = context.make_id(names, positions)
    for name in h.multi_split(names, [" | "]):
        person.add("name", name)
    person.add("citizenship", data.pop("Citizenship", None))
    person.add("taxNumber", data.pop("Tax Number", None))
    person.add("sourceUrl", data.pop("Links").split(" | "))
    archive_links = data.pop("Archive links", None)
    if archive_links is not None:
        for archive_link in archive_links.split(" | "):
            person.add("sourceUrl", archive_link)
    dob_pob = data.pop("Date and place of birth", None)
    if dob_pob:
        dp_parts = dob_pob.split(" | ")
        # If we get more than one part, unpack it into dob and pob
        if len(dp_parts) == 2:
            dob, pob = dp_parts
            h.apply_date(person, "birthDate", dob)
            person.add("birthPlace", pob)
        elif len(dp_parts) == 1:
            # If there’s only one part, we assume it's just the dob
            dob = dp_parts[0]
            h.apply_date(person, "birthDate", dob)
    if positions:
        for pos_parts in h.multi_split(positions, [" | ", " / "]):
            person.add("position", pos_parts)
    person.add("topics", "poi")

    sanction = h.make_sanction(context, person)
    sanction.add("reason", data.pop("Reasons").replace(" | ", " "))
    sanction.add("sourceUrl", link)

    context.emit(person, target=True)
    context.emit(sanction)
    context.audit_data(data)


def crawl_company(context: Context, details_container, link):
    data = {}
    for row in details_container.findall(".//div[@class='row']"):
        divs = row.findall("div")
        if len(divs) == 2:
            label_elem, value_elem = divs
            if "yellow" in label_elem.get("class"):
                data = extract_label_value_pair(label_elem, value_elem, data)

    name = data.pop("Name")
    name_abbr = data.pop("Abbreviated name of the legal entity", None)
    reg_num = data.pop("Registration number")

    company = context.make("Company")
    company.id = context.make_id(name, name_abbr, reg_num)
    company.add("name", name)
    company.add("name", name_abbr)
    company.add("registrationNumber", reg_num)
    company.add("address", data.pop("Address"))
    company.add("country", data.pop("Country"))
    company.add("taxNumber", data.pop("Tax Number"))
    company.add("sourceUrl", data.pop("Links").split(" | "))
    archive_links = data.pop("Archive links", None)
    if archive_links is not None:
        for archive_link in archive_links.split(" | "):
            company.add("sourceUrl", archive_link)

    company.add("topics", "poi")
    sanction = h.make_sanction(context, company)
    sanction.add("reason", data.pop("Reasons").replace(" | ", " "))
    sanction.add("sourceUrl", link)

    context.emit(company, target=True)
    context.emit(sanction)
    context.audit_data(data)


def extract_next_page_url(doc, base_url, next_xpath):
    doc.make_links_absolute(base_url)
    # next page <a> element extraction using xpath
    next_link_element = doc.xpath(next_xpath)

    if next_link_element:
        next_link = next_link_element[0]
        return next_link.get("href")

    return None


def crawl(context: Context):
    main_page = context.fetch_html(context.data_url, cache_days=3)
    node = main_page.find(
        ".//section[@class='sections d-flex flex-wrap align-items-stretch justify-content-center mb-5 pl-5 pr-5 medium']"
    )
    h.assert_dom_hash(node, "dbb9a924d940b3a69a132a102e04dcf0f9fbfc5e")

    for link_info in LINKS:
        base_url = link_info["url"]
        data_type = link_info["type"]
        current_url = base_url
        visited_pages = 0
        while current_url and visited_pages < 100:  # emergency exit check
            doc = context.fetch_html(current_url)
            if doc is None:
                context.log.warn(f"Failed to fetch {current_url}")
                break
            context.log.info(f"Processing {current_url}")
            crawl_index_page(context, current_url, data_type)

            # xpath to find the next page link
            next_xpath = "//ul[@class='pagination']//li[@class='next']/a"

            # get the next page URL, if exists
            next_url = extract_next_page_url(doc, base_url, next_xpath)

            if next_url:
                current_url = next_url
                visited_pages += 1
            else:
                break
        if visited_pages >= 100:
            raise Exception(
                "Emergency limit of 100 visited pages reached. Potential logical inconsistency detected."
            )
