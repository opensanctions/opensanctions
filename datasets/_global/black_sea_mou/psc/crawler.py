from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urljoin

from zavod import Context, helpers as h
from zavod.extract.llm import run_image_prompt
from zavod.stateful.positions import YEAR_DAYS
from zavod.shed.bs_tokyo_mou_psc import crawl_psc_record

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
    # Go back ~1 year
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
LLM_VERSION = "gpt-4o"
PROMPT = """This is an image of a numeric CAPTCHA.
Extract the 5-digit number shown in the image and return it as JSON: {"code": "XXXXX"}.
Preserve leading zeros. The answer is always exactly 5 digits."""


def crawl(context: Context) -> None:
    login_page = context.fetch_html(context.data_url)
    image = h.xpath_element(login_page, './/img[contains(@src, "captcha.php")]')
    captcha_url = urljoin(context.data_url, image.get("src"))
    image_path: Path = context.fetch_resource("captcha.png", captcha_url)
    context.log.debug(f"Fetched CAPTCHA image from {captcha_url} to {image_path}")
    result = run_image_prompt(
        context,
        prompt=PROMPT,
        image_path=image_path,
        cache_days=0,
        model=LLM_VERSION,
    )
    login_data = {"captcha": result["code"]}
    context.log.debug(f"Extracted CAPTCHA code: {result['code']} from the image")
    login_url = urljoin(context.data_url, "?action=login")
    login_resp = context.fetch_html(
        login_url, data=login_data, headers=HEADERS, method="POST"
    )
    assert login_resp is not None, "Login failed, response is None"

    total_pages = None
    page = 0
    while total_pages is None or page < total_pages:
        context.log.info(f"Crawling page {page} of {total_pages}")
        total_pages = crawl_psc_record(
            context,
            page=page,
            headers=HEADERS,
            search_data=SEARCH_DATA,
            getinspection_url=urljoin(context.data_url, "?action=getinspections"),
            getships_url=urljoin(context.data_url, "?action=getshipinsp"),
        )
        page += 1
