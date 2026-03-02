import re
from lxml import html
from typing import Optional

from zavod import Context, helpers as h


def make_search_data(page: int, search_data: dict) -> dict:
    return {**search_data, "Page": str(page)}


def parse_total_pages(tree: html.HtmlElement) -> Optional[int]:
    found_li = tree.xpath(
        "//ul[@class='navigate']/li[starts-with(normalize-space(.), 'Found')]"
    )
    if not found_li:
        return None  # No matching element found
    page_info_text = found_li[0].text_content()
    match = re.search(r"on (\d+) page", page_info_text)
    return int(match.group(1)) if match else None


def emit_unknown_link(
    context: Context, object: str | None, subject: str | None, role: str, date: str
) -> None:
    link = context.make("UnknownLink")
    link.id = context.make_id(object, subject, role)
    if role:
        link.add("role", role)
    link.add("subject", subject)
    link.add("object", object)
    h.apply_date(link, "date", date)
    context.emit(link)


def crawl_vessel_row(context: Context, str_row: dict, inspection_date: str) -> str:
    ship_name = str_row.pop("ship_name")
    imo = str_row.pop("imo_number")
    vessel = context.make("Vessel")
    vessel.id = context.make_id(ship_name, imo)
    vessel.add("name", ship_name)
    vessel.add("imoNumber", imo)
    vessel.add("type", str_row.pop("type"))
    vessel.add("callSign", str_row.pop("callsign"))
    vessel.add("mmsi", str_row.pop("mmsi"))
    vessel.add("grossRegisteredTonnage", str_row.pop("tonnage"))
    vessel.add("deadweightTonnage", str_row.pop("deadweight"))
    vessel.add("flag", str_row.pop("flag"))
    h.apply_date(vessel, "buildDate", str_row.pop("dateofkeellaid", None))
    context.emit(vessel)

    if captain := str_row.pop("name_of_ship_master", None):
        person = context.make("Person")
        person.id = context.make_id(captain, imo)
        person.add("name", captain)
        context.emit(person)
        emit_unknown_link(
            context,
            object=vessel.id,
            subject=person.id,
            role="Master",
            date=inspection_date,
        )

    if (
        class_soc := str_row.pop("classificationsociety")
    ) and class_soc.lower() != "other":
        org = context.make("Organization")
        org.id = context.make_id("org", class_soc)
        org.add("name", class_soc)
        context.emit(org)
        emit_unknown_link(
            context,
            object=vessel.id,
            subject=org.id,
            role="Classification society",
            date=inspection_date,
        )

    context.audit_data(str_row, ["date_keel_laid", "deadweight"])
    # Return vessel_id here so it can be processed in emit_unknown_link for company
    assert vessel.id is not None
    return vessel.id


def crawl_company_details(context: Context, str_row: dict) -> str:
    company_name = str_row.pop("name")
    company_imo = str_row.pop("imo_number")
    company = context.make("Company")
    company.id = context.make_slug(company_name, company_imo)
    company.add("name", company_name)
    company.add("imoNumber", company_imo)
    company.add("mainCountry", str_row.pop("registered"))
    company.add("jurisdiction", str_row.pop("residence"))
    company.add("email", str_row.pop("email"))
    company.add("phone", str_row.pop("phone"))
    context.emit(company)

    context.audit_data(str_row, ["fax"])
    assert company.id is not None
    return company.id


def crawl_vessel_page(
    context: Context,
    shipuid: str,
    headers: dict,
    getships_url: str,
) -> None:
    context.log.debug(f"Processing shipuid: {shipuid}")
    detail_data = {
        "MIME Type": "application/x-www-form-urlencoded",
        "UID": f"{shipuid}",
        "initiator": "insp",
    }

    # POST to get full ship profile using shipuid
    detail_doc = context.fetch_html(
        getships_url,
        data=detail_data,
        headers=headers,
        method="POST",
        cache_days=182,  # Cache for 6 months
    )
    inspection_table = h.xpath_element(
        detail_doc, "//h2[text()='Inspection data']/following-sibling::table[1]"
    )
    rows = list(h.parse_html_table(inspection_table))
    assert len(rows) == 1, len(rows)
    inspection_data = h.cells_to_str(rows[0])

    ship_data_table = h.xpath_element(
        detail_doc, "//h2[text()='Ship data']/following-sibling::table[1]"
    )
    rows = list(h.parse_html_table(ship_data_table))
    assert len(rows) == 1, len(rows)
    ship_data = h.cells_to_str(rows[0])
    assert inspection_data["date"] is not None, "Inspection date is missing"
    vessel_id = crawl_vessel_row(context, ship_data, inspection_data["date"])

    company_data = h.xpath_element(
        detail_doc, "//h2[text()='Company details']/following-sibling::table[1]"
    )
    for row in h.parse_html_table(company_data):
        str_row = h.cells_to_str(row)
        company_id = crawl_company_details(context, str_row)
        emit_unknown_link(
            context,
            object=vessel_id,
            subject=company_id,
            role="Company",
            date=inspection_data["date"],
        )
        context.audit_data(str_row, ["fax"])


def crawl_psc_record(
    context: Context,
    page: int,
    headers: dict,
    search_data: dict,
    getinspection_url: str,
    getships_url: str,
) -> int:
    doc = context.fetch_html(
        getinspection_url,
        data=make_search_data(page, search_data),
        headers=headers,
        method="POST",
    )
    # Parse the response to find shipuids
    shipuid_xpath = "//tr[contains(@class, 'even') or contains(@class, 'odd')]//input[@type='hidden']/@value"
    shipuids = h.xpath_strings(doc, shipuid_xpath)
    context.log.info(f"Found {len(shipuids)} shipuids in the search response")
    if len(shipuids) < 1:
        context.log.warn("Not enough shipuids found, double check the logic.")
    for shipuid in shipuids:
        crawl_vessel_page(context, str(shipuid), headers, getships_url)
    # Extract and return total pages
    total_pages = parse_total_pages(doc)
    assert total_pages is not None, "Failed to parse total pages"
    return total_pages
