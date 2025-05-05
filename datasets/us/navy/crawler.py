from lxml import html, etree
from urllib.parse import urljoin
import orjson
import re

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise
from zavod.shed.zyte_api import fetch_html

BASE_URL = "https://www.navy.mil"
API_URL = "https://www.navy.mil/API/ArticleCS/Public/GetList?moduleID=709&dpage=%d&TabId=119&language=en-US"

TITLE_REGEX = re.compile(
    r"^(Dr\.|Rear Admiral|Vice Admiral|Rear Adm\.|Admiral|Adm\.)\s+(.+)$"
)

CATEGORY_URLS = [
    "https://www.navy.mil/Leadership/Flag-Officer-Biographies/",
    "https://www.secnav.navy.mil/donhr/About/Senior-Executives/Pages/Biographies.aspx",
]


def extract_name_and_title(context, raw_name: str) -> tuple[str, str | None]:
    match = TITLE_REGEX.match(raw_name)
    if match:
        # Return the name and title separately
        return match.group(2).strip(), match.group(1).strip()
    context.log.info("Failed to extract title from name:", name=raw_name)
    return raw_name, None


def emit_person(
    context: Context,
    country: str,
    source_url: str,
    role: str,
    name: str,
    title: str,
    notes: str,
):
    person = context.make("Person")
    person.id = context.make_id(country, name, role)
    person.add("name", name)
    person.add("position", role)
    person.add("sourceUrl", source_url)
    person.add("title", title)
    person.add("notes", notes)

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

    name, title = extract_name_and_title(context, name)
    emit_person(context, "us", url, role, name, title=title, notes="")


def parse_json_or_xml(context: Context, url: str, data: str):
    try:
        json_doc = orjson.loads(data)
        return json_doc
    except orjson.JSONDecodeError:
        context.log.info(f"Failed to decode JSON from {url}, trying as XML instead")

    try:
        root = etree.fromstring(data)
        html_data = root.xpath(".//*[local-name() = 'data']")[0].text
        done = root.xpath(".//*[local-name() = 'done']")[0].text
        return {"data": html_data, "done": done}
    except etree.XMLSyntaxError as e:
        context.log.info(f"Failed to parse XML for {url}: {e}")


def process_page(context: Context, page_number: int):
    url = API_URL % page_number
    try:
        data = context.fetch_text(url, headers={"Accept": "application/xml"})
    except Exception as e:
        context.log.exception(f"Failed to fetch JSON from {url}: {e}")
        return False, False

    if not data:
        context.log.error(f"No data found for page {page_number}")
        return False, False

    doc = parse_json_or_xml(context, url, data)

    if not doc:
        context.log.error(f"No parseable data found for page {page_number}")
        return False, False

    items = html.fromstring(doc["data"]).findall(".//li")
    for item in items:
        item_html = html.tostring(item, encoding="unicode")
        crawl_person(context, item_html)

    return True, doc["done"] == "true"


def parse_html(context):
    section_xpath = './/div[contains(@class, "DNNModuleContent") and contains(@class, "ModDNNHTMLC")]'
    doc = fetch_html(context, context.data_url, section_xpath, cache_days=3)
    for section in doc.xpath(section_xpath):
        for row in section.xpath('.//div[@class="row"]'):
            # Extract core HTML elements
            name_el = row.xpath(".//h1/a")
            role_el = row.xpath(".//h3/a")
            notes_el = row.xpath('.//p[contains(@class, "bio-sum")]')

            if not name_el or not role_el:
                context.log.warning(
                    f"Skipping incomplete leader entry:{html.tostring(row, pretty_print=True, encoding="unicode")}"
                )
                continue
            # Extract text content
            raw_name = name_el[0].text_content().strip()
            role = role_el[0].text_content().strip()
            notes = notes_el[0].text_content().strip() if notes_el else ""
            leader_url = urljoin(BASE_URL, name_el[0].get("href"))

            name, title = extract_name_and_title(context, raw_name)
            if not name or not role:
                context.log.warning("Missing name or role:", name=name, role=role)
                continue
            emit_person(context, "us", leader_url, role, name, title=title, notes=notes)


def crawl(context: Context):
    page_number = 0
    done = False
    while not done:
        context.log.info(f"Fetching page {page_number}")
        success, done_flag = process_page(context, page_number)
        if not success:
            # They have a really wonky cache that sometimes serves up XML, sometimes JSON,
            # sometimes broken data, so just try again once.
            success, done_flag = process_page(context, page_number)
            if not success:
                context.log.error(f"Failed to fetch page {page_number}, bailing out")
                return

        done = done_flag
        page_number += 1

    # Parse additional page
    parse_html(context)
