import re

from lxml import html
from pprint import pprint
from typing import Optional

from zavod import Context, helpers as h

# imo = "8817007"
HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Referer": "https://apcis.tmou.org/public/",
    "User-Agent": "Mozilla/5.0",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://apcis.tmou.org",
}
SEARCH_DATA = {
    "Param": "0",
    "callsign": "",
    "name": "",
    "compimo": "",
    "compname": "",
    "From": "25.05.2025",
    "Till": "25.06.2025",
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


def crawl_vessel(context: Context, shipuid: str):
    print(f"Processing shipuid: {shipuid}")
    detail_data = {
        "MIME Type": "application/x-www-form-urlencoded",
        "UID": f"{shipuid}",
        "initiator": "insp",
    }

    # Step 4: POST to get full ship profile using shipuid
    detail_resp = context.http.post(
        "https://apcis.tmou.org/public/?action=getshipinsp",
        data=detail_data,
        headers=HEADERS,
    )
    detail_resp.raise_for_status()
    tree = html.fromstring(detail_resp.text)
    tables = tree.xpath("//table[@class='table']")
    assert len(tables) >= 3, "Expected at least 3 tables in the response"
    ship_data = tables[1]
    for row in h.parse_html_table(ship_data):
        str_row = h.cells_to_str(row)
        pprint(str_row)

    company_data = tables[2]
    for row in h.parse_html_table(company_data):
        str_row = h.cells_to_str(row)
        pprint(str_row)


def crawl_page(context: Context, page: int, search_tree: html.HtmlElement):
    if page == 0:
        tree = search_tree  # use already-fetched tree
    else:
        resp = context.http.post(
            "https://apcis.tmou.org/public/?action=getinspections",
            data=make_search_data(page),
            headers=HEADERS,
        )
        tree = html.fromstring(resp.text)

    shipuids = tree.xpath(
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

    login_data = {
        "user": "apcismgr",
        "password": "mgrsicpa11",
        "captcha": answer,
    }
    login_url = "https://apcis.tmou.org/public/?action=login"
    login_resp = context.http.post(login_url, data=login_data, headers=HEADERS)
    print("Login status:", login_resp.status_code)

    # Step 2: Load PSC inspection page
    # First, fetch page 0 to get total_pages
    search_data = make_search_data(0)
    search_resp = context.http.post(
        "https://apcis.tmou.org/public/?action=getinspections",
        data=search_data,
        headers=HEADERS,
    )

    # Step 3: Extract the shipuid from the getinspections response
    search_tree = html.fromstring(search_resp.text)
    total_pages = parse_total_pages(context, search_tree)
    assert total_pages is not None, "Failed to parse total pages"
    for page in range(0, total_pages):
        crawl_page(context, page, search_tree)
