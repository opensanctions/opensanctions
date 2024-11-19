from zavod import Context, helpers as h


LINKS = [
    # {  # child kidnappers
    #     "url": "https://war-sanctions.gur.gov.ua/en/kidnappers/persons?page={page}&per-page=12",
    #     "max_pages": 26,
    #     "type": "person",
    # },
    # {  # child kidnappers
    #     "url": "https://war-sanctions.gur.gov.ua/en/kidnappers/companies?page={page}&per-page=12",
    #     "max_pages": 14,
    #     "type": "company",
    # },
    {  # russian athletes
        "url": "https://war-sanctions.gur.gov.ua/en/sport/persons?page={page}&per-page=12",
        "max_pages": 9,
        "type": "person",
    },
    # {  # ships
    #     "url": "https://war-sanctions.gur.gov.ua/en/transport/ships?page={page}&per-page=12",
    #     "max_pages": 40,
    #     "type": "vessel",
    # },
]


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
    data = {}
    rows = details_container.xpath(
        ".//div[contains(@class,'tools-spec')]/div[contains(@class, 'row')]"
    )
    for row in rows:
        label_elem = row.find(".//div[@class='col-12 col-lg-6 text-lg-right']")
        value_elem = row.find(".//div[@class='js_visibility_target']")
        if value_elem is None:
            value_elem = row.find(".//span[@class='js_visibility_target']")

        if label_elem is not None and value_elem is not None:
            label = label_elem.text_content().strip().replace("\n", " ")
            value = value_elem.text_content().strip().replace("\n", " ")
            value = " ".join(value.split())
            value = " | ".join(
                [text.strip() for text in value_elem.itertext() if text.strip()]
            ).strip()
            data[label] = value

    justification = details_container.xpath(
        ".//div[contains(@class, 'tools-frame')]/div[contains(@class, 'mb-3')]"
    )
    for row in justification:
        divs = row.findall("div")

        if len(divs) == 2:  # Ensure there are exactly two divs in a row
            label_elem, value_elem = divs

            if "yellow" in value_elem.get("class", ""):
                # Clean and extract text
                label = label_elem.text_content().strip().replace("\n", " ").strip()
                value = value_elem.text_content().strip().replace("\n", " ").strip()
                value = " ".join(value.split())
                value = " | ".join(
                    [text.strip() for text in value_elem.itertext() if text.strip()]
                ).strip()

                # Store in the dictionary
                data[label] = value

    additional_info = details_container.xpath(
        ".//div[contains(@class, 'tools-frame')]//div[@class='mb-3' or contains(@class, 'js_visibility')]"
    )
    for row in additional_info:
        divs = row.findall("div")

        if len(divs) == 2:
            label_elem, value_elem = divs

            if "yellow" in value_elem.get("class", ""):
                label = label_elem.text_content().strip().replace("\n", " ").strip()
                value = value_elem.text_content().strip().replace("\n", " ").strip()
                value = " ".join(value.split())
                value = " | ".join(
                    [text.strip() for text in value_elem.itertext() if text.strip()]
                ).strip()
                data[label] = value
            # print(data)

    web_resources = []
    web_links = details_container.xpath(
        ".//div[contains(@class, 'tools-frame')]//a[contains(@class, 'd-block long-text yellow mb-3')]"
    )
    for raw_link in web_links:
        link_href = raw_link.get("href", "").strip()
        web_resources.append(link_href)
    data["Web resources"] = web_resources

    name = data.pop("Vessel name (international according to IMO)")
    type = data.pop("Vessel Type")
    imo_num = data.pop("IMO")
    description = data.pop("Category")
    ais_shutdown = data.pop("Cases of AIS shutdown")
    ru_ports = data.pop("Calling at russian ports")
    ports = data.pop("Visited ports", None)
    com_manager = data.pop("Commercial ship manager (IMO / Country / Date)")
    safety_manager = data.pop(
        "Ship Safety Management Manager (IMO / Country / Date)", None
    )
    build_country = data.pop("Builder (country)")
    flags_former = data.pop("Flags (former)")

    vessel = context.make("Vessel")
    vessel.id = context.make_id(name, imo_num)
    vessel.add("name", name)
    vessel.add("imoNumber", imo_num)
    vessel.add("type", type)
    vessel.add("description", description)
    vessel.add("description", data.pop("Vessel information"))
    vessel.add("description", f"Cases of AIS shutdown: {ais_shutdown}")
    vessel.add("description", f"Calling at russian ports: {ru_ports}")
    vessel.add("description", f"Visited ports: {ports}")
    vessel.add("description", f"Builder (country): {build_country}")
    vessel.add("description", f"Flags (former): {flags_former}")
    vessel.add("callSign", data.pop("Call sign"))
    vessel.add("flag", data.pop("Flag (Current)"))
    vessel.add("mmsi", data.pop("MMSI"))
    vessel.add("buildDate", data.pop("Build year"))
    for web_resource in data.pop("Web resources"):
        vessel.add("sourceUrl", web_resource)
    for name in h.multi_split(data.pop("Former ship names"), [" / "]):
        vessel.add("previousName", name)
    vessel.add(
        "description", f"Commercial ship manager (IMO / Country / Date): {com_manager}"
    )
    vessel.add(
        "description",
        f"Ship Safety Management Manager (IMO / Country / Date): {safety_manager}",
    )
    vessel.add("topics", "sanction")

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
        linked_entity.add("name", linked_entity_name)
        linked_entity.add("topics", "sanction.linked")
        context.emit(linked_entity, target=True)

    owner_info = data.pop("Shipowner (IMO / Country / Date)")
    if owner_info != "":
        owner_parts = owner_info.split(" / ")
        if len(owner_parts) == 3:
            owner_name, owner_country, owner_date = owner_parts
            owner = context.make("LegalEntity")
            owner.id = context.make_id(owner_name, owner_country)
            owner.add("name", owner_name)
            owner.add("country", owner_country)
            owner.add("topics", "sanction.linked")
            context.emit(owner, target=True)

            ownership = context.make("Ownership")
            ownership.id = context.make_id(vessel.id, owner.id)
            ownership.add("asset", vessel.id)
            ownership.add("owner", owner.id)
            ownership.add("ownershipType", "Owner")

            context.emit(ownership)

    context.audit_data(data)


def crawl_person(context: Context, details_container, link):
    data = {}
    for row in details_container.findall(".//div[@class='row']"):
        label_elem = row.find(
            ".//div[@class='col-12 col-md-4 col-lg-2 yellow']"
        )  # get children and assert one with yellow and 1 without (2 in total)
        value_elem = row.find(".//div[@class='col-12 col-md-8 col-lg-10']")
        if value_elem is None:
            value_elem = row.find(
                ".//div[@class='js_visibility_target col-12 col-md-8 col-lg-10']"
            )
        if label_elem is not None and value_elem is not None:
            label = label_elem.text_content().strip().replace("\n", " ")
            value = value_elem.text_content().strip().replace("\n", " ")
            value = " ".join(value.split())
            value = " | ".join(
                [text.strip() for text in value_elem.itertext() if text.strip()]
            ).strip()
            data[label] = value
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
        pos_parts = positions.split(" / ")
        for position in pos_parts:
            person.add("position", position)
    person.add("topics", "sanction")
    person.add("topics", "crime.war")

    sanction = h.make_sanction(context, person)
    sanction.add("reason", data.pop("Reasons"))
    sanction.add("sourceUrl", link)

    context.emit(person, target=True)
    context.emit(sanction)
    context.audit_data(data)


def crawl_company(context: Context, details_container, link):
    data = {}
    for row in details_container.findall(".//div[@class='row']"):
        label_elem = row.find(".//div[@class='col-12 col-sm-4 yellow']")
        value_elem = row.find(".//div[@class='col-12 col-sm-8']")

        if label_elem is not None and value_elem is not None:
            label = label_elem.text_content().strip().replace("\n", " ")
            value = value_elem.text_content().strip().replace("\n", " ")
            value = " ".join(value.split())
            value = " | ".join(
                [text.strip() for text in value_elem.itertext() if text.strip()]
            ).strip()
            data[label] = value

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

    company.add("topics", "sanction")
    sanction = h.make_sanction(context, company)
    sanction.add("reason", data.pop("Reasons"))
    sanction.add("sourceUrl", link)

    context.emit(company, target=True)
    context.emit(sanction)
    context.audit_data(data)


def extract_next_page_url(doc, base_url, next_xpath):
    doc.make_links_absolute(base_url)
    # next page <a> element extraction using XPath
    next_link_element = doc.xpath(next_xpath)

    if next_link_element:
        next_link = next_link_element[0]
        return next_link.get("href")

    return None


def crawl(context):
    for link_info in LINKS:
        base_url = link_info["url"]
        data_type = link_info["type"]

        current_url = base_url.format(page=1)

        visited_pages = 0
        max_pages = link_info["max_pages"]

        while current_url and visited_pages < max_pages * 3:  # Emergency exit check
            # Fetch the page using context
            doc = context.fetch_html(current_url)
            if doc is None:
                print(f"Failed to fetch {current_url}")
                break
            context.log.info(f"Processing {current_url}")
            crawl_index_page(context, current_url, data_type)

            # Define the XPath to find the next page link
            # Ensure `<a>` elements are selected
            next_xpath = "//ul[@class='pagination']//li[@class='next']/a"

            # Get the next page URL, if exists
            next_url = extract_next_page_url(doc, base_url, next_xpath)

            if next_url:
                current_url = next_url
                visited_pages += 1
            else:
                break
