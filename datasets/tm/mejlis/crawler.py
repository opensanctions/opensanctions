import re
from typing import Any

from lxml.html import HtmlElement
from requests.exceptions import HTTPError
from urllib3.util import Retry

from zavod import Context, helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.extract.zyte_api import fetch_html

DEPUTY_RE = re.compile(r"single-deputy/(\d+)")
YEAR_RE = re.compile(r"^\d{4}$")


def fetch_html_with_retry(context: Context, url: str, **kwargs: Any) -> HtmlElement:
    # Zyte answers 421 "Website Connection Error" when its exit node fails to open a
    # connection to mejlis.gov.tm. As of 2026-09-25, a few percent of requests through
    # the automatically chosen exits fail this way and TM exits fail every time, while
    # DE exits seem fine for the moment. This may change in the future as the winds of
    # BGP change direction.
    retry = Retry(total=5, backoff_factor=3)
    while True:
        try:
            return fetch_html(context, url, geolocation="DE", **kwargs)
        except HTTPError as err:
            if err.response is None or err.response.status_code != 421:
                raise
            context.log.info("Zyte connection error, retrying", url=url)
            # retry.increment raises MaxRetryError once the retries are exhausted
            retry = retry.increment(url=url, error=err)
            retry.sleep()


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
    doc = fetch_html_with_retry(
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
        wikidata_id="Q21328577",
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    doc = fetch_html_with_retry(
        context,
        f"{context.data_url}?lang=en",
        unblock_validator="//a[contains(@href, 'single-deputy/')]",
        cache_days=1,
    )
    deputy_ids = parse_deputy_ids(doc)

    for deputy_id in deputy_ids:
        crawl_deputy(context, deputy_id, position, categorisation)
