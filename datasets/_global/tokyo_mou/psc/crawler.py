import re
from datetime import datetime, timedelta
from urllib.parse import urljoin

from zavod import Context, helpers as h
from zavod.stateful.positions import YEAR_DAYS
from zavod.shed.bs_tokyo_mou_psc import crawl_psc_records

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
    # Go back ~1 year (approximate as 365 days)
    "From": f"{(TODAY - timedelta(days=YEAR_DAYS)).strftime('%d.%m.%Y')}",
    "Till": f"{TODAY.strftime('%d.%m.%Y')}",
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


def crawl(context: Context) -> None:
    # Submit login form
    login_page = context.fetch_html(context.data_url)
    # Solve the arithmetic CAPTCHA
    question = h.xpath_string(login_page, "//span[contains(text(), '=')]/text()").strip(
        " ="
    )
    answer = solve_arithmetic(question)

    login_data = {"captcha": answer}
    login_resp = context.fetch_html(
        urljoin(context.data_url, "?action=login"),
        data=login_data,
        headers=HEADERS,
        method="POST",
    )
    assert login_resp is not None, "Login failed, response is None"

    crawl_psc_records(
        context,
        headers=HEADERS,
        search_data=SEARCH_DATA,
        getinspection_url=urljoin(context.data_url, "?action=getinspections"),
        getships_url=urljoin(context.data_url, "?action=getshipinsp"),
    )
