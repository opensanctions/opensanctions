import re
from urllib.parse import urljoin

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import (
    PositionCategorisation,
    categorise,
)
from zavod.util import Element

ID_RE = re.compile(r"id=(\d+)")
YEAR_RE = re.compile(r"\d{4}")


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


def list_periods(context: Context, position: Entity, doc: Element) -> list[str]:
    """Return the in-window parliamentary-period ids from the listing's selector.

    The roster can be filtered to any past parliamentary period via the
    ``idRegistroPadre`` query parameter.
    """
    periods: list[str] = []
    for option in h.xpath_elements(
        doc, ".//select[@name='idRegistroPadre']/option[@value]"
    ):
        value = option.get("value")
        if not value:
            continue
        years = [int(y) for y in YEAR_RE.findall(h.element_text(option))]
        if len(years) > 0 and min(years) < int(
            h.earliest_term_start(position.get("topics"))[:4]
        ):
            context.log.info(
                "Skipping out-of-window period", period=h.element_text(option)
            )
            continue
        periods.append(value)
    if len(periods) == 0:
        raise ValueError("No parliamentary periods found in the listing selector")
    return periods


def crawl_member(
    context: Context,
    detail_url: str,
    email: str | None,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    doc = context.fetch_html(detail_url, cache_days=7)

    name = field_value(doc, "nombres")

    # "Periodo de Funciones" gives the Inicio (start) and Término (end) of the period
    # the member actually served — the full legislative term for an ordinary member, or
    # the sub-period for a replacement. These are the term/period bounds, recorded as
    # the occupancy's periodStart/periodEnd (the source exposes no individual end date).
    period_dates = h.xpath_strings(
        doc,
        ".//p[@class='periodo']//span[@class='periododatos']/span[@class='value']/text()",
    )
    period_start = period_dates[0]
    period_end = period_dates[1]

    person = context.make("Person")
    person.id = context.make_id(name, detail_url)
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
        period_start=period_start,
        period_end=period_end,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    occupancy.add("constituency", field_value(doc, "representa"))  # electoral district
    occupancy.add("politicalGroup", field_value(doc, "bancada"))  # parliamentary group

    context.emit(occupancy)
    context.emit(person)


def crawl_period(
    context: Context,
    period_id: str,
    position: Entity,
    categorisation: PositionCategorisation,
    seen: set[str],
) -> None:
    doc = context.fetch_html(
        context.data_url,
        params={"idRegistroPadre": period_id},
        absolute_links=True,
        cache_days=1,
    )
    links = h.xpath_elements(doc, './/a[@class="conginfo"]')
    if len(links) == 0:
        raise ValueError(f"No congressperson links found for period {period_id}")

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

        context.log.info(
            f"Crawling period {period_id} congressperson {index}/{len(links)}",
            url=detail_url,
        )
        crawl_member(context, detail_url, email, position, categorisation)
        # Persist the HTTP cache periodically so a long run keeps its progress.
        context.flush()


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, absolute_links=True, cache_days=1)

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

    # Crawl every parliamentary period exposed by the roster (current and historical).
    seen: set[str] = set()
    for period_id in list_periods(context, position, doc):
        crawl_period(context, period_id, position, categorisation, seen)
