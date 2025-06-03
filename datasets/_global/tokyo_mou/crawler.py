import httpx
import re

from lxml import etree
from lxml import html
from pprint import pprint
from time import sleep

from zavod import Context

imo = "8817007"
MAX_RETRIES = 5
RETRY_DELAY = 5  # seconds


def solve_arithmetic(expression: str) -> str:
    match = re.search(r"(\d+)\s*([+\-*/])\s*(\d+)", expression)
    if not match:
        raise ValueError(f"Invalid CAPTCHA expression: {expression}")
    a, op, b = match.groups()
    return str(eval(f"{a}{op}{b}"))


def crawl(context: Context):
    with httpx.Client(http2=False, follow_redirects=True) as client:
        # Step 1: Load login page
        login_page = client.get("https://apcis.tmou.org/public/")
        tree = html.fromstring(login_page.text)

        # Step 2: Extract arithmetic from HTML
        question = tree.xpath("string(//span[contains(text(), '=')])").strip(" =")
        print(f"CAPTCHA question: {question}")
        answer = solve_arithmetic(question)

        # Step 3: Submit login form
        login_data = {
            "user": "apcismgr",
            "password": "mgrsicpa11",
            "captcha": answer,
        }

        login_resp = client.post(
            "https://apcis.tmou.org/public/?action=login",
            data=login_data,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://apcis.tmou.org/public/",
                "X-Requested-With": "XMLHttpRequest",
                "Origin": "https://apcis.tmou.org",
            },
        )

        print("Login response snippet:")
        print(login_resp.text[:500])

        # Step 4: Search ships by IMO
        search_data = {
            "Param": "0",
            "Value": imo,
            "imo": imo,
            "callsign": "",
            "name": "",
            "compimo": "",
            "compname": "",
            "From": "03.05.2025",
            "Till": "03.06.2025",
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

        for attempt in range(MAX_RETRIES):
            search_resp = client.post(
                "https://apcis.tmou.org/public/?action=getships",
                data=search_data,
                headers={
                    "Accept": "*/*",
                    "Accept-Encoding": "gzip, deflate, br",
                    "Accept-Language": "en-GB,en;q=0.9",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Origin": "https://apcis.tmou.org",
                    "Referer": "https://apcis.tmou.org/public/",
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Safari/605.1.15",
                    "X-Requested-With": "XMLHttpRequest",
                },
            )
            if search_resp.status_code == 200 and search_resp.text.strip():
                break  # success!
            print(
                f"[warn] Retry {attempt + 1}/{MAX_RETRIES}: Server returned {search_resp.status_code}, retrying in {RETRY_DELAY}s"
            )
            sleep(RETRY_DELAY)
        else:
            raise RuntimeError("Failed to get valid search response after retries.")

        # Step 5: Extract the shipuid from the getships response
        search_tree = html.fromstring(search_resp.text)
        print(etree.tostring(search_tree, pretty_print=True, encoding="unicode"))
        shipuid = search_tree.xpath(
            "string(//tr[contains(@class, 'even')]//input/@value)"
        )
        if not shipuid:
            raise RuntimeError("Failed to extract shipuid from search response")
        print(f"Extracted shipuid: {shipuid}")
        detail_data = {"shipuid": shipuid}

        # Step 6: POST to get full ship profile using shipuid
        for attempt in range(1, MAX_RETRIES + 2):  # +2 because range is exclusive
            try:
                detail_resp = client.post(
                    "https://apcis.tmou.org/public/?action=getship",
                    data=detail_data,
                    headers={
                        "Content-Type": "application/x-www-form-urlencoded",
                        "Referer": "https://apcis.tmou.org/public/",
                        "User-Agent": "Mozilla/5.0",
                        "X-Requested-With": "XMLHttpRequest",
                    },
                )
                detail_resp.raise_for_status()
                pprint(detail_resp.text)
                if detail_resp.text.strip():  # Make sure it's not empty
                    break  # success
                else:
                    raise ValueError("Empty response")

            except Exception as e:
                context.log.warning(f"Retry {attempt}/{MAX_RETRIES}: {e}")
                if attempt == MAX_RETRIES + 1:
                    raise RuntimeError(
                        "Failed to fetch detailed ship info after retries."
                    )
                sleep(RETRY_DELAY)
