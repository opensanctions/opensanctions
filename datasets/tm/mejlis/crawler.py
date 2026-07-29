import re
from time import sleep
from lxml import etree
from lxml.html import HtmlElement
from requests.exceptions import HTTPError

from zavod import Context, helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.extract.zyte_api import fetch_html

DEPUTY_RE = re.compile(r"single-deputy/(\d+)")
YEAR_RE = re.compile(r"^\d{4}$")
# Zyte answers 421 /website/connection-error when its proxy cannot reach the
# site at all. Connectivity to Turkmen government hosts is intermittent, and
# zavod only retries 413/429/503/520 for Zyte requests, so without this a
# single blip on one of the 125 sequential fetches aborts the whole run.
CONNECTION_ERROR = 421
CONNECTION_RETRIES = 3
BACKOFF_FACTOR = 10


def fetch_html_retrying(
    context: Context,
    url: str,
    unblock_validator: str,
    html_source: str = "browserHtml",
    cache_days: int | None = None,
) -> etree._Element:
    """Fetch a page, retrying when Zyte cannot connect to the site."""
    attempt = 0
    while True:
        try:
            return fetch_html(
                context,
                url,
                unblock_validator=unblock_validator,
                html_source=html_source,
                cache_days=cache_days,
            )
        except HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status != CONNECTION_ERROR or attempt >= CONNECTION_RETRIES:
                raise
            attempt += 1
            pause = BACKOFF_FACTOR * 2**attempt
            context.log.info(
                f"Zyte could not connect to the site, sleeping {pause}s then retrying",
                url=url,
                attempt=attempt,
            )
            sleep(pause)


def parse_deputy_ids(doc: HtmlElement) -> list[str]:
    """Collect the deputy ids linked from the convocation roster page, in order."""
    ids: list[str] = []
    for href in h.xpath_strings(doc, "//a[contains(@href, 'single-deputy/')]/@href"):
        match = DEPUTY_RE.search(href)
        if match is not None and match.group(1) not in ids:
            ids.append(match.group(1))
    return ids


def field_value(right_block: HtmlElement, label: str) -> str | None:
    """Return the text of the profile's `label:` row value, if present."""
    for row in h.xpath_elements(
        right_block, ".//div[contains(@class, 'key_value_wrapper')]"
    ):
        key = h.element_text(h.xpath_element(row, ".//span[@class='key']")).rstrip(":")
        if key == label:
            return h.element_text(
                h.xpath_element(row, ".//*[contains(@class, 'value')]")
            )
    return None


def crawl_deputy(
    context: Context,
    deputy_id: str,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    url = f"https://mejlis.gov.tm/single-deputy/{deputy_id}"
    right_block_xpath = "//div[contains(@class, 'right_block')]"
    doc = fetch_html_retrying(
        context,
        f"{url}?lang=en",
        unblock_validator=right_block_xpath,
        html_source="httpResponseBody",
        cache_days=7,
    )
    right_block = h.xpath_element(doc, right_block_xpath)
    name = h.element_text(h.xpath_element(right_block, ".//h3[@class='name']"))

    person = context.make("Person")
    person.id = context.make_slug("deputy", deputy_id)
    h.apply_name(person, full=name, lang="eng")
    person.add("sourceUrl", f"{url}?lang=en")
    # Deputies of the Mejlis must be citizens of Turkmenistan (Constitution art. 120).
    # https://www.constituteproject.org/constitution/Turkmenistan_2016
    person.add("citizenship", "tm")

    year = field_value(right_block, "Year of Birth")
    if year is not None and YEAR_RE.match(year) is not None:
        h.apply_date(person, "birthDate", year)

    paragraphs = [
        h.element_text(p)
        for p in h.xpath_elements(right_block, ".//div[@class='deputy_bio_text']//p")
    ]
    person.add("biography", "\n".join(p for p in paragraphs if len(p) > 0))

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return

    # electoral constituency: the named election district
    district = h.xpath_elements(right_block, ".//span[@class='district_name']")
    if len(district) > 0:
        occupancy.add("constituency", h.element_text(district[0]))
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Mejlis of Turkmenistan",
        country="tm",
        topics=["gov.national", "gov.legislative"],
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    doc = fetch_html_retrying(
        context,
        f"{context.data_url}?lang=en",
        unblock_validator="//a[contains(@href, 'single-deputy/')]",
        cache_days=1,
    )
    deputy_ids = parse_deputy_ids(doc)

    for deputy_id in deputy_ids:
        crawl_deputy(context, deputy_id, position, categorisation)
