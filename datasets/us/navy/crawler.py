from lxml import html, etree
from urllib.parse import urljoin
import orjson
import re

from zavod import Context, helpers as h
from zavod.logic.pep import categorise
from zavod.shed.zyte_api import fetch_html

BASE_URL = "https://www.navy.mil"
API_URL = "https://www.navy.mil/API/ArticleCS/Public/GetList?moduleID=709&dpage=%d&TabId=119&language=en-US"

TITLE_REGEX = re.compile(
    r"^(.*)\b(Admiral|Adm\.)\s+(.*)$"
)  # Regular expression to match either "Admiral" or "Adm."

# Allow-list of URLs that are allowed to be skipped
ALLOWED_LEADER_URLS = [
    "https://www.navy.mil/Leadership/Flag-Officer-Biographies/",
    "https://www.secnav.navy.mil/donhr/About/Senior-Executives/Pages/Biographies.aspx",
]


def emit_person(
    context: Context, country: str, source_url: str, role: str, name: str, title: str
):
    person = context.make("Person")
    person.id = context.make_id(country, name, role)
    person.add("name", name)
    person.add("position", role)
    person.add("sourceUrl", source_url)
    person.add("title", title)

    position = h.make_position(context, role, country=country, topics=["gov.security"])

    categorisation = categorise(context, position, is_pep=True)
    if categorisation.is_pep:
        occupancy = h.make_occupancy(context, person, position)
        if occupancy:
            context.emit(person)
            context.emit(position)
            context.emit(occupancy)


def crawl_person(context: Context, item_html: str) -> None:
    doc = html.fromstring(item_html)
    link_el = doc.find(".//a")
    if link_el is None:
        return
    url = link_el.get("href")
    name = link_el.find(".//h2").text_content().strip()
    role = link_el.find(".//h3").text_content().strip()

    match = re.search(TITLE_REGEX, name)
    if match:
        # Extract the full title (including everything before "Admiral" or "Adm.")
        title = match.group(1).strip() + " " + match.group(2).strip()
        # Extract the name (everything after "Admiral" or "Adm.")
        name = match.group(3).strip()
    else:
        # Log a warning if no title is found
        context.log.info(f"Failed to extract title from name: {name}")
        title = None
    emit_person(context, "us", url, role, name, title=title)


def process_page(context: Context, page_number: int):
    url = API_URL % page_number
    try:
        data = context.fetch_json(url)
    except orjson.JSONDecodeError as e:
        context.log.error(f"Failed to decode JSON from {url}: {e}")
        return False, False
    except Exception as e:
        context.log.error(f"Failed to fetch JSON from {url}: {e}")
        return False, False

    if not data:
        context.log.error(f"No data found for page {page_number}")
        return False, False

    if isinstance(data, dict):
        html_data = data.get("data")
        done = data.get("done", True)
    else:
        try:
            root = etree.fromstring(data)
            html_data = root.find(".//data").text
            done = root.find(".//done").text.lower() == "true"
        except etree.XMLSyntaxError as e:
            context.log.error(f"Failed to parse XML for page {page_number}: {e}")
            return False, False

    if not html_data:
        context.log.error(f"No HTML data found for page {page_number}")
        return False, done

    items = html.fromstring(html_data).findall(".//li")
    for item in items:
        item_html = html.tostring(item, encoding="unicode")
        crawl_person(context, item_html)

    return True, done


def unblock_validator(doc: html.HtmlElement) -> bool:
    return len(doc.xpath(".//div[contains(@class, 'DNNModuleContent')]")) > 0


def parse_html(context):
    section_xpath = './/div[contains(@class, "DNNModuleContent") and contains(@class, "ModPhotoDashboardC")]'
    doc = fetch_html(context, context.data_url, section_xpath, cache_days=3)
    for div in doc.xpath(section_xpath):
        leader_divs = div.xpath('.//div[contains(@class, "leader-title")]/a')
        for leader_div in leader_divs:
            leader_url = urljoin(BASE_URL, leader_div.get("href"))
            # Check if the URL is in the allow-list, skip it if true
            if leader_url in ALLOWED_LEADER_URLS:
                continue
            name_element = leader_div.find(".//h3")
            role_element = leader_div.find(".//h2")
            if name_element is None or role_element is None:
                context.log.warning(
                    f"Skipping incomplete leader entry: {html.tostring(leader_div, pretty_print=True, encoding='unicode')}"
                )
                continue
            name = name_element.text_content().strip()
            match = re.search(TITLE_REGEX, name)
            if match:
                # Extract the full title (including everything before "Admiral" or "Adm.")
                title = match.group(1).strip() + " " + match.group(2).strip()
                # Extract the name (everything after "Admiral" or "Adm.")
                name = match.group(3).strip()
            else:
                # Log a warning if no title is found
                context.log.info(f"Failed to extract title from name: {name}")
                title = None
            role = role_element.text_content().strip()
            assert name and role, f"Name or role missing: {name}, {role}"
            emit_person(context, "us", leader_url, role, name, title=title)


def crawl(context: Context):
    page_number = 0
    done = False
    error_pages = []
    while not done:
        context.log.info(f"Fetching page {page_number}")
        success, done_flag = process_page(context, page_number)
        if not success:
            error_pages.append(page_number)
        done = done_flag
        page_number += 1

    # Retry the error pages
    for page_number in error_pages:
        context.log.info(f"Retrying page {page_number}")
        success, _ = process_page(context, page_number)
        if not success:
            context.log.error(f"Failed again for page {page_number}")

    # Parse additional page
    parse_html(context)
