import re
from urllib.parse import urljoin

from zavod import Context, settings
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import (
    OccupancyStatus,
    PositionCategorisation,
    categorise,
)
from zavod.util import Element

ID_RE = re.compile(r"id=(\d+)")
POSITION_TOPICS = ["gov.national", "gov.legislative"]
# Members flagged with this condition are currently in office; any other condition
# (Fallecido, Destituído, Suspendido, Inactivo, ...) means they have left the seat.
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


def parse_iso(context: Context, raw: str | None) -> str | None:
    """Parse a source date (e.g. ``26-Jul-2021``) to an ISO string, or None."""
    if raw is None:
        return None
    parsed = h.extract_date(context.dataset, raw, fallback_to_original=False)
    return parsed[0] if len(parsed) > 0 else None


def crawl_member(
    context: Context,
    detail_url: str,
    email: str | None,
    position: Entity,
    categorisation: PositionCategorisation,
    cutoff: str,
) -> None:
    doc = context.fetch_html(detail_url, cache_days=7)

    condition = field_value(doc, "condicion")
    if condition is None:
        raise ValueError(f"No condition found at {detail_url}")
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
    start_date, term_end = period_dates

    if condition.casefold() == ACTIVE_CONDITION:
        # Sitting member: Término is the scheduled term end (in the future) and
        # make_occupancy resolves the occupancy as current.
        status = None
        end_date: str | None = term_end
    else:
        # The member has left office. Término is usually still the scheduled term end
        # rather than the actual departure, so only keep it as the end date when it is
        # already in the past (e.g. a date of death); otherwise the end is unknown.
        status = OccupancyStatus.ENDED
        term_end_iso = parse_iso(context, term_end)
        today = settings.RUN_TIME.date().isoformat()
        end_date = term_end if term_end_iso is not None and term_end_iso < today else None

    # Skip members who left office before our PEP coverage window.
    end_iso = parse_iso(context, end_date)
    if end_iso is not None and end_iso < cutoff:
        return

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
        status=status,
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
    cutoff = h.earliest_term_start(POSITION_TOPICS)
    doc = context.fetch_html(context.data_url, absolute_links=True, cache_days=1)
    links = h.xpath_elements(doc, './/a[@class="conginfo"]')
    if len(links) == 0:
        raise ValueError("No congressperson links found on the listing page")

    position = h.make_position(
        context,
        name="Member of the Congress of the Republic of Peru",
        country="pe",
        topics=POSITION_TOPICS,
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
        crawl_member(context, detail_url, email, position, categorisation, cutoff)
        # Persist the HTTP cache periodically so a long run keeps its progress.
        context.flush()
