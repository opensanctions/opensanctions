import re
from urllib.parse import urljoin

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element

ID_RE = re.compile(r"id=(\d+)")
# Only members currently in office are emitted; the roster also lists members who
# have left (e.g. "Fallecido", "Suspendido").
ACTIVE_CONDITION = "en ejercicio"


def field_value(el: Element, css_class: str) -> str | None:
    """Return the text of the ``.value`` span inside the ``<p class=...>`` block.

    The Ficha de Congresista renders each datum as
    ``<p class='X'><span class='field'>Label:</span><span class='value'>Y</span></p>``.
    Returns None when the block is absent; raises if it appears more than once so an
    unexpected layout change fails loudly.
    """
    spans = h.xpath_elements(el, f".//p[@class='{css_class}']/span[@class='value']")
    if len(spans) == 0:
        return None
    if len(spans) > 1:
        raise ValueError(f"Multiple '{css_class}' values found")
    return h.element_text(spans[0]) or None


def crawl_member(
    context: Context,
    detail_url: str,
    email: str | None,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    doc = context.fetch_html(detail_url, cache_days=7)

    condition = field_value(doc, "condicion")
    if condition is None:
        raise ValueError(f"No condition found at {detail_url}")
    if condition.casefold() != ACTIVE_CONDITION:
        context.log.info("Skipping member not in office", condition=condition)
        return

    name = field_value(doc, "nombres")
    if name is None:
        raise ValueError(f"No name found at {detail_url}")

    # Inicio (start) and Término (end) of the period of functions, in that order.
    period_dates = h.xpath_strings(
        doc,
        ".//p[@class='periodo']//span[@class='periododatos']/span[@class='value']/text()",
    )
    if len(period_dates) != 2:
        raise ValueError(f"Expected start and end dates at {detail_url}")
    start_date, end_date = period_dates

    person = context.make("Person")
    person.id = context.make_slug(ID_RE.search(detail_url).group(1))  # type: ignore[union-attr]
    person.add("name", name)
    # Members of Congress must be Peruvian by birth (Constitution of Peru, Art. 90).
    # https://www.constituteproject.org/constitution/Peru_2021
    person.add("citizenship", "pe")
    person.add("political", field_value(doc, "grupo"))
    person.add("email", email)
    person.add("sourceUrl", detail_url)

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        start_date=start_date,
        end_date=end_date,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    occupancy.add("constituency", field_value(doc, "representa"))
    # The bancada (parliamentary group) is distinct from party membership.
    occupancy.add("politicalGroup", field_value(doc, "bancada"))

    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, absolute_links=True, cache_days=1)
    links = h.xpath_elements(doc, './/a[@class="conginfo"]')
    if len(links) == 0:
        raise ValueError("No congressperson links found on the listing page")

    position = h.make_position(
        context,
        name="Member of the Congress of the Republic of Peru",
        country="pe",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q18812470",
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    seen: set[str] = set()
    for index, link in enumerate(links, start=1):
        href = link.get("href")
        if href is None or ID_RE.search(href) is None:
            raise ValueError(f"Congressperson link without id: {href!r}")
        detail_url = urljoin(context.data_url, href)
        if detail_url in seen:
            continue
        seen.add(detail_url)

        # Email is only available on the listing row, next to the member's link.
        row = h.xpath_element(link, "ancestor::tr[1]")
        emails = h.xpath_strings(row, './/a[starts-with(@href, "mailto:")]/@href')
        email = emails[0].removeprefix("mailto:") if len(emails) > 0 else None

        context.log.info(f"Crawling congressperson {index}/{len(links)}", url=detail_url)
        crawl_member(context, detail_url, email, position, categorisation)
        # Persist the HTTP cache periodically so a long run keeps its progress.
        context.flush()
