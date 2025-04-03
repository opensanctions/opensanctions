import re
from typing import Optional

from zavod import Context, helpers as h
from zavod.entity import Entity
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
    {  # stealers of heritage
        "url": "https://war-sanctions.gur.gov.ua/en/stolen/persons",
        "type": "person",
        "program": "Persons involved in the theft and destruction of Ukrainian cultural heritage",
    },
    {  # stealers of heritage
        "url": "https://war-sanctions.gur.gov.ua/en/stolen/companies",
        "type": "legal_entity",
        "program": "Legal entities involved in the theft and destruction of Ukrainian cultural heritage",
    },
]

# e.g. Ocean Dolphin Ship Management (6270796
# or   Ocean Dolphin Ship Co. c/o Ocean Ship Management LLC (6270796
REGEX_SHIP_PARTY = re.compile(
    r"""
    ^(?P<name>.+?)  # non-greedy to prevent matching the c/o part
    ( [cсп]/[oо]:?\ (?P<care_of>[^\(]+)\ )?
    \(
    (?P<imo_number>.+)$
    """,
    re.VERBOSE,
)


def extract_label_value_pair(label_elem, value_elem, data):
    label = label_elem.text_content().strip().replace("\n", " ")
    value = [text.strip() for text in value_elem.itertext() if text.strip()]
    if len(value) == 1:
        value = value[0]
    data[label] = value
    return label, value


def apply_life_dates(date_str, entity):
    parts = h.multi_split(str(date_str), [" - "])
    if len(parts) > 1:
        h.apply_date(entity, "birthDate", parts[0])
        h.apply_date(entity, "deathDate", parts[1])
        return True
    return False


def apply_dob_pob(context, entity, dob_pob):
    if not dob_pob:
        return
    # Handle list with two elements [dob, pob]
    if isinstance(dob_pob, list) and len(dob_pob) == 2:
        dob, pob = dob_pob
        if not apply_life_dates(dob, entity):
            h.apply_date(entity, "birthDate", dob)
        entity.add("birthPlace", pob)
    # Handle string format (single date or date range)
    elif isinstance(dob_pob, str):
        if not apply_life_dates(dob_pob, entity):
            h.apply_date(entity, "birthDate", dob_pob)
    else:
        context.log.warning(f"Unexpected dob_pob format: {dob_pob}")


def crawl_index_page(context: Context, index_page, data_type, program):
    index_page = fetch_html(
        context,
        index_page,
        unblock_validator=".//div[@id='main-grid']",
        html_source="httpResponseBody",
        # cache_days=1,  # only for dev
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
            apply_dob_pob(context, captain, dob_pob)

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

    name = data.pop("Vessel name")
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
    deadweight_tonnage = data.pop("DWT")
    if deadweight_tonnage != "0":
        vessel.add("tonnage", deadweight_tonnage)
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

    pi_club = data.pop("P&I Club")
    if pi_club != "-":
        emit_relation(context, vessel, pi_club, rel_role="P&I Club")

    crawl_ship_relation(
        context,
        vessel,
        data.pop("Commercial ship manager (IMO / Country / Date)", None),
        "Commercial ship manager",
    )
    crawl_ship_relation(
        context,
        vessel,
        data.pop("Ship Safety Management Manager (IMO / Country / Date)", None),
        "Ship Safety Management Manager",
    )
    crawl_ship_relation(
        context,
        vessel,
        data.pop("Ship Owner (IMO / Country / Date)", None),
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
            "The person in connection with whomsanctions have been applied",
        ],
    )


def name_looks_unclean(name):
    return any(
        [
            dodgy
            for dodgy in ["/", "current", "former", "previous", "("]
            if dodgy in name.lower()
        ]
    )


def parse_ship_party(relation_info):
    relation_parts = relation_info.split(" / ")
    if len(relation_parts) == 3:
        entity_name_number, entity_country, entity_date = relation_parts
        match = REGEX_SHIP_PARTY.match(entity_name_number)
        if match and not name_looks_unclean(match.group("name")):
            entity_name = match.group("name").strip()
            imo_number = match.group("imo_number")
            care_of = match.group("care_of")
            return {
                "name": entity_name,
                "imo_number": imo_number,
                "country": entity_country,
                "date": entity_date,
                "care_of": care_of,
            }
    return None


def crawl_ship_relation(
    context: Context,
    vessel: Entity,
    relation_info,
    rel_role: Optional[str] = None,
    rel_schema: str = "UnknownLink",
    from_prop: str = "subject",
    to_prop: str = "object",
):
    if relation_info is None:
        return
    if result := parse_ship_party(relation_info):
        entity_name = result["name"]
        imo_number = result["imo_number"]
        country = result["country"]
        entity_date = result["date"]
        care_of = result["care_of"]
    else:
        res = context.lookup("ship_party", relation_info)
        if res:
            if res.name is None:
                return
            entity_name = res.name
            imo_number = res.imo_number
            country = res.country
            entity_date = res.date
            care_of = res.care_of
        else:
            context.log.warning(
                f"Couldn't parse vessel-related party '{relation_info}'",
                string=relation_info,
            )
            return
    other_entity = emit_relation(
        context,
        vessel,
        entity_name,
        country,
        imo_number,
        rel_schema,
        rel_role,
        from_prop,
        to_prop,
        entity_date,
    )

    if care_of is not None:
        emit_relation(
            context,
            other_entity,
            care_of,
            rel_role="c/o",
            from_prop="object",
            to_prop="subject",
        )


def emit_relation(
    context: Context,
    entity: Entity,
    name: str,
    country: Optional[str] = None,
    imo_number: Optional[str] = None,
    rel_schema: str = "UnknownLink",
    rel_role: Optional[str] = None,
    from_prop: str = "subject",
    to_prop: str = "object",
    start_date: Optional[str] = None,
) -> Entity:
    other = context.make("LegalEntity")
    other.id = context.make_id(name, country)
    other.add("name", name)
    other.add_cast("Organization", "imoNumber", imo_number)
    other.add("country", country)
    context.emit(other)

    relation = context.make(rel_schema)
    relation.id = context.make_id(entity.id, rel_role, other.id)
    relation.add(from_prop, entity.id)
    relation.add(to_prop, other.id)
    relation.add("role", rel_role)
    h.apply_date(relation, "startDate", start_date)
    context.emit(relation)

    return other


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
    dob = data.pop("Date of birth", None)
    archive_links = data.pop("Archive links", None)

    person = context.make("Person")
    person.id = context.make_id(names, positions)
    for name in h.multi_split(names, [" | "]):
        person.add("name", name)
    person.add("citizenship", data.pop("Citizenship", None))
    person.add("taxNumber", data.pop("Tax Number", None))
    person.add("sourceUrl", data.pop("Links", None))
    person.add("position", data.pop("Other positions", None))
    if dob:
        h.apply_date(person, "birthDate", dob)
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
        apply_dob_pob(context, person, dob_pob)
    if positions:
        for position in h.multi_split(positions, [" / "]):
            person.add("position", position)

    sanction = h.make_sanction(context, person)
    sanction.add("reason", data.pop("Reasons", None))
    sanction.add("sourceUrl", link)
    sanction.add("program", program)

    context.emit(person)
    context.emit(sanction)
    context.audit_data(
        data, ignore=["Sanction Jurisdictions", "Permission for illegal excavations"]
    )


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
    # Child kidnappers
    # Components in weapons
    # Instruments of war
    # Marine and aircraft vessels
    # Stolen heritage
    # Partner's sanctions lists
    # Champions of terror
    # Kremlin mouthpieces
    # UAV manufacturers
    # Executives of war
    h.assert_dom_hash(
        section_links_section[0], "6d9e5bb137fbbd3c5698008f0c01ed10318d9b53"
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
    # - Captains - in LINKS
    # - Shadow Fleet - in LINKS
    # - Air vessels - "soon"
    # - Ports - "soon"
    # - Aircraft - "soon"
    h.assert_dom_hash(
        transport_tabs_container[0], "da1210b8efb6480150ca06b95e7f22b19b018f44"
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
                # cache_days=1,  # only for dev
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
