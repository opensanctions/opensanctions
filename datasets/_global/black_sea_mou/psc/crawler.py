import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import urljoin

from zavod import Context, helpers as h
from zavod.extract.llm import run_image_prompt
from zavod.stateful.positions import YEAR_DAYS
from zavod.shed.bs_tokyo_mou_psc import crawl_psc_records

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
MAX_LOGIN_ATTEMPTS = 5


def attempt_login(context: Context) -> None:
    """Fetch a fresh CAPTCHA and attempt login, establishing the session cookie."""
    login_page = context.fetch_html(context.data_url)
    image = h.xpath_element(login_page, './/img[contains(@src, "captcha.php")]')
    captcha_url = urljoin(context.data_url, image.get("src"))
    # Fetch the CAPTCHA into a tempfile rather than via fetch_resource: the latter
    # skips the download when the target file already exists, which on a retry would
    # reuse a stale image that can never match the freshly rotated session CAPTCHA.
    response = context.fetch_response(captcha_url)
    with tempfile.NamedTemporaryFile(suffix=".png") as tmp:
        tmp.write(response.content)
        tmp.flush()
        context.log.debug(f"Fetched CAPTCHA image from {captcha_url}")
        try:
            result = run_image_prompt(
                context,
                prompt=PROMPT,
                image_path=Path(tmp.name),
                cache_days=0,
                model=LLM_VERSION,
            )
            code = result["code"]
        except (AssertionError, KeyError) as exc:
            # The LLM occasionally returns no/malformed content for a CAPTCHA image.
            # Treat that like any other login failure so the retry loop fetches a
            # fresh CAPTCHA and tries again, rather than crashing the whole run.
            raise ValueError(f"CAPTCHA extraction failed: {exc!r}") from exc
    context.log.debug(f"Extracted CAPTCHA code: {code}")
    login_url = urljoin(context.data_url, "?action=login")
    # The server returns an empty body on wrong CAPTCHA, HTML on success.
    login_text = context.fetch_text(
        login_url, data={"captcha": code}, headers=HEADERS, method="POST"
    )
    if not login_text:
        raise ValueError(
            f"Login failed: server returned empty response (wrong CAPTCHA code: {code})"
        )


def crawl(context: Context) -> None:
    for attempt in range(1, MAX_LOGIN_ATTEMPTS + 1):
        try:
            context.log.info(f"Login attempt {attempt}/{MAX_LOGIN_ATTEMPTS}")
            attempt_login(context)
            break
        except ValueError as exc:
            context.log.warning(str(exc))
            if attempt == MAX_LOGIN_ATTEMPTS:
                raise RuntimeError(
                    f"Login failed after {MAX_LOGIN_ATTEMPTS} attempts"
                ) from exc

    crawl_psc_records(
        context,
        headers=HEADERS,
        search_data=SEARCH_DATA,
        getinspection_url=urljoin(context.data_url, "?action=getinspections"),
        getships_url=urljoin(context.data_url, "?action=getshipinsp"),
    )
