from collections import namedtuple
from typing import Any, Mapping, Optional
from lxml import html, etree
from urllib.parse import urljoin
import orjson
import re

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise
from zavod.extract import zyte_api
from zavod.extract.zyte_api import ZyteAPIRequest

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
    source_url: str | None,
    role: str,
    name: str,
    title: str | None,
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
    name = h.xpath_strings(link_el, ".//h2/text()", expect_exactly=1)[0]
    role = h.xpath_strings(link_el, ".//h3/text()", expect_exactly=1)[0]

    name, title = extract_name_and_title(context, name)
    emit_person(context, "us", url, role, name, title=title, notes="")


def parse_json_or_xml(
    context: Context, url: str, data: str
) -> Optional[Mapping[str, Any]]:
    try:
        root = etree.fromstring(data)
        html_data = root.xpath(".//*[local-name() = 'data']")[0].text
        done = root.xpath(".//*[local-name() = 'done']")[0].text
        return {"data": html_data, "done": done}
    except etree.XMLSyntaxError as e:
        context.log.debug(f"Failed to parse XML for {url}: {e}, trying as JSON instead")

    try:
        json_doc = orjson.loads(data)
        return json_doc
    except orjson.JSONDecodeError:
        context.log.debug(f"Failed to decode JSON from {url}")


ProcessPageResult = namedtuple(
    "ProcessPageResult",
    [
        # Whether the page was successfully fetched
        "success",
        # Whether the iteration in the pagination is done. If false, the next page should be fetched.
        "done",
    ],
)


def process_page(context: Context, page_number: int) -> ProcessPageResult:
    url = API_URL % page_number
    try:
        resp = zyte_api.fetch(
            context, ZyteAPIRequest(url=url, headers={"Accept": "application/xml"})
        )
    except Exception as e:
        context.log.exception(f"Failed to fetch response from {url}: {e}")
        return ProcessPageResult(success=False, done=False)

    if not resp.response_text:
        context.log.error(f"No data found for page {page_number}")
        resp.invalidate_cache(context)
        return ProcessPageResult(success=False, done=False)

    doc = parse_json_or_xml(context, url, resp.response_text)

    if not doc:
        context.log.error(f"No parseable data found for page {page_number}")
        resp.invalidate_cache(context)
        return ProcessPageResult(success=False, done=False)

    items = html.fromstring(doc["data"]).findall(".//li")
    for item in items:
        item_html = html.tostring(item, encoding="unicode")
        crawl_person(context, item_html)

    return ProcessPageResult(success=True, done=doc["done"] == "true")


def parse_html(context):
    section_xpath = './/div[contains(@class, "DNNModuleContent") and contains(@class, "ModDNNHTMLC")]'
    doc = zyte_api.fetch_html(
        context, context.data_url, section_xpath, geolocation="us", cache_days=3
    )
    for section in h.xpath_elements(doc, section_xpath):
        for row in h.xpath_elements(section, './/div[@class="row"]'):
            # Extract core HTML elements
            name_el = h.xpath_elements(row, ".//h1/a", expect_exactly=1)[0]
            raw_name = h.xpath_strings(row, ".//h1/a/text()", expect_exactly=1)[0]
            role = h.xpath_strings(row, ".//h3/a/text()", expect_exactly=1)[0]
            notes = h.xpath_string(row, './/p[contains(@class, "bio-sum")]/text()')
            leader_url = urljoin(BASE_URL, name_el.get("href"))

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
