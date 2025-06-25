import re

from lxml import etree
from lxml import html
from pprint import pprint

from zavod import Context

# imo = "8817007"
HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Referer": "https://apcis.tmou.org/public/",
    "User-Agent": "Mozilla/5.0",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://apcis.tmou.org",
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
    search_data = {
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
    search_resp = context.http.post(
        "https://apcis.tmou.org/public/?action=getinspections",
        data=search_data,
        headers=HEADERS,
    )

    # Step 3: Extract the shipuid from the getinspections response
    search_tree = html.fromstring(search_resp.text)
    print(etree.tostring(search_tree, pretty_print=True, encoding="unicode"))
    shipuid = search_tree.xpath("string(//tr[contains(@class, 'even')]//input/@value)")
    if not shipuid:
        raise RuntimeError("Failed to extract shipuid from search response")
    print(f"Extracted shipuid: {shipuid}")
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
    pprint(detail_resp.text)
