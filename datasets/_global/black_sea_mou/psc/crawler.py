import re

from lxml import html
from typing import Optional
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

from zavod import Context, helpers as h
from zavod.extract.llm import run_image_prompt

TODAY = datetime.today()
HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Referer": "https://bsis.bsmou.org/public/?action=login",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
    "Origin": "https://bsis.bsmou.org",
}
SEARCH_DATA = {
    "imo": "",
    "callsign": "",
    "name": "",
    "From": "26.01.2026",  # f"{(TODAY - timedelta(days=YEAR_DAYS)).strftime('%d.%m.%Y')}",
    "Till": "26.02.2026",  # f"{TODAY.strftime('%d.%m.%Y')}",
    "authority": "0",
    "flag": "0",
    "class": "0",
    "ro": "0",
    "type": "0",
    "result": "0",
    "insptype": "-1",
    "sort1": "0",
    "sort2": "DESC",
    "sort3": "0",
    "sort4": "DESC",
}
LLM_VERSION = "gpt-4o"
PROMPT = """This is an image of a numeric CAPTCHA.
Extract the 5-digit number shown in the image and return it as JSON: {"code": "XXXXX"}.
Preserve leading zeros. The answer is always exactly 5 digits."""

# TODO: move most of the code to 'contrib' and reuse from there for both Tokyo MOU and Black Sea MOU, since they are almost identical.


def make_search_data(page):
    return {**SEARCH_DATA, "Page": str(page)}


def parse_total_pages(context, tree: html.HtmlElement) -> Optional[int]:
    found_li = tree.xpath(
        "//ul[@class='navigate']/li[starts-with(normalize-space(.), 'Found')]"
    )
    if not found_li:
        return None  # No matching element found
    page_info_text = found_li[0].text_content()
    match = re.search(r"on (\d+) page", page_info_text)
    return int(match.group(1)) if match else None


def emit_unknown_link(context, object, subject, role, date: str):
    link = context.make("UnknownLink")
    link.id = context.make_id(object, subject, role)
    if role:
        link.add("role", role)
    link.add("subject", subject)
    link.add("object", object)
    h.apply_date(link, "date", date)
    context.emit(link)


def crawl_vessel_row(context: Context, str_row: dict, inspection_date: str):
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
    h.apply_date(vessel, "buildDate", str_row.pop("dateofkeellaid"))
    context.emit(vessel)

    if captain := str_row.pop("name_of_ship_master"):
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

    if class_soc := str_row.pop("classificationsociety"):
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
    return vessel.id


def crawl_company_details(context: Context, str_row: dict):
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
    return company.id


def crawl_vessel_page(context: Context, shipuid: str):
    context.log.debug(f"Processing shipuid: {shipuid}")
    detail_data = {
        "MIME Type": "application/x-www-form-urlencoded",
        "UID": f"{shipuid}",
        "initiator": "insp",
    }

    # POST to get full ship profile using shipuid
    detail_doc = context.fetch_html(
        "https://bsis.bsmou.org/public/?action=getshipinsp",
        data=detail_data,
        headers=HEADERS,
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
    for row in h.parse_html_table(company_data[0]):
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


def crawl_list_page(context: Context, page: int):
    doc = context.fetch_html(
        "https://bsis.bsmou.org/public/?action=getinspections",
        data=make_search_data(page),
        headers=HEADERS,
        method="POST",
    )
    # Parse the response to find shipuids
    shipuids = doc.xpath(
        "//tr[contains(@class, 'even') or contains(@class, 'odd')]//input[@type='hidden']/@value"
    )
    context.log.info(f"Found {len(shipuids)} shipuids in the search response")
    if len(shipuids) < 2:
        context.log.warn("Not enough shipuids found, double check the logic.")
    for shipuid in shipuids:
        crawl_vessel_page(context, shipuid)
    # Extract and return total pages
    total_pages = parse_total_pages(context, doc)
    assert total_pages is not None, "Failed to parse total pages"
    print(f"Processed page {page}, total pages: {total_pages}")
    return total_pages


def crawl(context: Context):
    login_page = context.fetch_html(context.data_url)
    image = h.xpath_element(login_page, './/img[contains(@src, "captcha.php")]')
    captcha_url = urljoin(context.data_url, image.get("src"))
    image_path: Path = context.fetch_resource("captcha.png", captcha_url)
    result = run_image_prompt(
        context,
        prompt=PROMPT,
        image_path=image_path,
        cache_days=0,
        model=LLM_VERSION,
    )
    login_data = {"captcha": result["code"]}
    login_url = urljoin(context.data_url, "?action=login")
    login_resp = context.fetch_html(
        login_url, data=login_data, headers=HEADERS, method="POST"
    )
    assert login_resp is not None, "Login failed, response is None"

    total_pages = None
    page = 0
    while total_pages is None or page < total_pages:
        total_pages = crawl_list_page(context, page)
        page += 1
