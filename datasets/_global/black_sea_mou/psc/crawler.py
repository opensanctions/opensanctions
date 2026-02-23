from datetime import datetime, timedelta
from pathlib import Path

from zavod import Context, helpers as h
from zavod.extract.llm import run_image_prompt
from zavod.stateful.positions import YEAR_DAYS

TODAY = datetime.today()
HEADERS = {
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-GB,en;q=0.9",
    "Content-Type": "application/x-www-form-urlencoded",
    "Referer": "https://bsis.bsmou.org/public/?button=Agree",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://bsis.bsmou.org",
}


LLM_VERSION = "gpt-4o"
PROMPT = """This is an image of a numeric CAPTCHA.
Extract the 5-digit number shown in the image and return it as JSON: {"code": "XXXXX"}.
Preserve leading zeros. The answer is always exactly 5 digits."""


SEARCH_DATA = {
    "imo": "0",
    "callsign": "",
    "name": "",
    "From": "23.01.2026",  # f"{(TODAY - timedelta(days=YEAR_DAYS)).strftime('%d.%m.%Y')}",
    "Till": "23.02.2026",  # f"{TODAY.strftime('%d.%m.%Y')}",
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


def make_search_data(page):
    return {**SEARCH_DATA, "Page": str(page)}


def crawl_list_page(context: Context, page: int):
    doc = context.fetch_html(
        "https://bsis.bsmou.org/public/?action=getinspections",
        data=make_search_data(page),
        headers=HEADERS,
        method="POST",
    )
    # Parse the response to find shipuids
    shipuids = doc.xpath(
        "///tr[contains(@class, 'even') or contains(@class, 'odd')]//input[@type='hidden']/@value"
    )
    context.log.info(f"Found {len(shipuids)} shipuids in the search response")


def crawl(context: Context):
    login_page = context.fetch_html(context.data_url, cache_days=1, absolute_links=True)
    # image = h.xpath_element(login_page, '//img[@src="captcha.php"]')
    # captcha_url = image.get("src")
    # print(f"Captcha URL: {captcha_url}")
    # assert captcha_url is not None
    image_path: Path = context.fetch_resource(
        "captcha.png", "https://bsis.bsmou.org/public/captcha.php"
    )
    result = run_image_prompt(
        context,
        prompt=PROMPT,
        image_path=image_path,
        cache_days=0,
        model=LLM_VERSION,
    )
    login_data = {"captcha": result["code"]}
    login_url = "https://bsis.bsmou.org/public/?action=login"
    login_resp = context.fetch_html(
        login_url, data=login_data, headers=HEADERS, method="POST"
    )
    assert login_resp is not None, "Login failed, response is None"

    total_pages = None
    page = 0
    while total_pages is None or page < total_pages:
        total_pages = crawl_list_page(context, page)
        page += 1
