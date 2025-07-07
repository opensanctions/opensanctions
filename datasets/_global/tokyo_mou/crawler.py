import re
from lxml import html
from typing import Optional
from datetime import datetime, timedelta

from zavod import Context, helpers as h

TODAY = datetime.today()
HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Referer": "https://apcis.tmou.org/public/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://apcis.tmou.org",
}
SEARCH_DATA = {
    "Param": "0",
    "callsign": "",
    "name": "",
    "compimo": "",
    "compname": "",
    # Go back ~6 months (approximate as 182 days)
    "From": f"{(TODAY - timedelta(days=182)).strftime("%d.%m.%Y")}",
    "Till": f"{TODAY.strftime("%d.%m.%Y")}",
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


def solve_arithmetic(expression: str) -> str:
    """Parse and solve a simple arithmetic question like '7 + 8'."""
    match = re.search(r"^(\d+)\s*([+\-*/])\s*(\d+)$", expression)
    if not match:
        raise ValueError(f"Invalid CAPTCHA expression: {expression}")
    a, op, b = match.groups()
    a, b = int(a), int(b)
    if op == "+":
        return str(a + b)
    if op == "-":
        return str(a - b)
    raise ValueError(f"Unknown op: {op}")


def make_search_data(page):
    data = dict(SEARCH_DATA)
    data["Page"] = str(page)
    return data


def parse_total_pages(context, tree: html.HtmlElement) -> Optional[int]:
    found_li = tree.xpath(
        "//ul[@class='navigate']/li[starts-with(normalize-space(.), 'Found')]"
    )
    if not found_li:
        return None  # No matching element found
    page_info_text = found_li[0].text_content()
    match = re.search(r"on (\d+) page", page_info_text)
    return int(match.group(1)) if match else None


def emit_unknown_link(context, vessel_id, org_id, role):
    link = context.make("UnknownLink")
    link.id = context.make_id(vessel_id, org_id, role)
    if role:
        link.add("role", role)
    link.add("subject", org_id)
    link.add("object", vessel_id)
    context.emit(link)


def crawl_ship_data(context: Context, str_row: dict):
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
    # TODO: map the 'deadweight' once we have a property for it
    # TODO: add the topic (most likely 'mar.detained') once we have it
    vessel.add("flag", str_row.pop("flag"))
    context.emit(vessel)

    class_soc = str_row.pop("classificationsociety")
    if class_soc:
        org = context.make("Organization")
        org.id = context.make_id("org", class_soc)
        org.add("name", class_soc)
        context.emit(org)

        emit_unknown_link(context, vessel.id, org.id, "Classification society")
    context.audit_data(str_row, ["date_keel_laid", "deadweight"])
    # We return the vessel_id here so it can be processed in 'crawl_company_details'
    return vessel.id


def crawl_company_details(context: Context, str_row: dict, vessel_id):
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

    emit_unknown_link(context, vessel_id, company.id, None)
    context.audit_data(str_row, ["fax"])


def crawl_vessel(context: Context, shipuid: str):
    print(f"Processing shipuid: {shipuid}")
    detail_data = {
        "MIME Type": "application/x-www-form-urlencoded",
        "UID": f"{shipuid}",
        "initiator": "insp",
    }

    # Step 4: POST to get full ship profile using shipuid
    detail_resp = context.fetch_html(
        "https://apcis.tmou.org/public/?action=getshipinsp",
        data=detail_data,
        headers=HEADERS,
        method="POST",
        # cache_days=1,
    )
    tables = detail_resp.xpath("//table[@class='table']")
    assert len(tables) >= 3, "Expected at least 3 tables in the response"
    ship_data = detail_resp.xpath(
        "//h2[text()='Ship data']/following-sibling::table[1]"
    )
    assert len(ship_data) == 1, "Expected exactly one ship data table"
    for row in h.parse_html_table(ship_data[0]):
        str_row = h.cells_to_str(row)
        vessel_id = crawl_ship_data(context, str_row)

    company_data = detail_resp.xpath(
        "//h2[text()='Company details']/following-sibling::table[1]"
    )
    assert len(company_data) == 1, "Expected exactly one company data table"
    for row in h.parse_html_table(company_data[0]):
        str_row = h.cells_to_str(row)
        crawl_company_details(context, str_row, vessel_id)


def crawl_page(context: Context, page: int, doc: html.HtmlElement):
    if page == 0:
        doc = doc  # use already-fetched tree
    else:
        doc = context.fetch_html(
            "https://apcis.tmou.org/public/?action=getinspections",
            data=make_search_data(page),
            headers=HEADERS,
            method="POST",
            # cache_days=1,
        )

    shipuids = doc.xpath(
        "///tr[contains(@class, 'even') or contains(@class, 'odd')]//input[@type='hidden']/@value"
    )
    print(f"Found {len(shipuids)} shipuids in the search response")
    if len(shipuids) < 15:
        context.log.warn("Not enough shipuids found, double check the logic.")
    for shipuid in shipuids:
        crawl_vessel(context, shipuid)


def crawl(context: Context):
    # Step 1: Submit login form
    login_page = context.http.get("https://apcis.tmou.org/public/")
    tree = html.fromstring(login_page.text)
    # Solve the arithmetic CAPTCHA
    question = tree.xpath("string(//span[contains(text(), '=')])").strip(" =")
    answer = solve_arithmetic(question)

    login_data = {"captcha": answer}
    login_url = "https://apcis.tmou.org/public/?action=login"
    login_resp = context.http.post(login_url, data=login_data, headers=HEADERS)
    print("Login status:", login_resp.status_code)

    # Step 2: Load PSC inspection page
    # First, fetch page 0 to get total_pages
    search_data = make_search_data(0)
    search_resp = context.fetch_html(
        "https://apcis.tmou.org/public/?action=getinspections",
        data=search_data,
        headers=HEADERS,
        method="POST",
        # cache_days=1,
    )

    # Step 3: Extract the shipuid from the getinspections response
    total_pages = parse_total_pages(context, search_resp)
    print(f"Total pages found: {total_pages}")
    assert total_pages is not None, "Failed to parse total pages"
    for page in range(0, total_pages):  # inclusive of 0, exclusive of total_pages
        crawl_page(context, page, search_resp)
