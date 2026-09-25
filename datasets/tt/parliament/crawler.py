from urllib.parse import urlencode

from zavod.entity import Entity
from zavod.stateful.positions import (
    OccupancyStatus,
    PositionCategorisation,
    categorise,
)
from zavod.util import Element

from zavod import Context
from zavod import helpers as h

HOUSES = ["House of Representatives", "Senate"]


def parse_term(label: str) -> tuple[str | None, str | None]:
    """Split a label like "12th Republican Parliament (28 Aug 2020 - 18 Mar 2025)"
    into its raw start and end dates. The sitting term ("Current - ...") has none."""
    if label.startswith("Current"):
        return None, None
    _, _, dates = label.partition("(")
    start, sep, end = dates.rstrip(")").partition(" - ")
    if not sep:
        raise ValueError(f"Cannot parse parliament term: {label!r}")
    return start, end


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    row: Element,
    period_start: str | None,
    period_end: str | None,
) -> None:
    raw_name = h.xpath_string(row, './/div[contains(@class, "member_name")]/text()')
    name = h.strip_name_titles(context, raw_name)

    person = context.make("Person")
    person.id = context.make_id(raw_name)
    person.add("name", name, original_value=raw_name if name != raw_name else None)
    for party in h.xpath_strings(
        row, './/div[starts-with(@class, "member_affiliation")]/text()'
    ):
        res = context.lookup("party", party)
        person.add("political", party if res is None else res.value)
    # Constitution of the Republic of Trinidad and Tobago, ss. 41 (Senate) and 47
    # (House of Representatives), both requiring Trinidad and Tobago citizenship:
    # https://www.constituteproject.org/constitution/Trinidad_and_Tobago_2007
    person.add("citizenship", "tt")
    person.add("sourceUrl", h.xpath_string(row, "./@href"))

    status = None
    # The flag describes the person, not the term, so it only tells us about the
    # sitting parliament; past terms are left to their end date.
    if period_end is None:
        flag = h.xpath_string(row, './/div[contains(@class, "current_status")]/text()')
        value = context.lookup_value(
            "occupancy_status", flag, "unknown", warn_unmatched=True
        )
        status = OccupancyStatus(value)
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        period_start=period_start,
        period_end=period_end,
        categorisation=categorisation,
        status=status,
    )
    if occupancy is None:
        return
    constituency = h.xpath_strings(
        row, './/div[contains(@class, "member_constituency")]/text()'
    )
    # The Speaker's row holds their office where others have a constituency.
    if constituency != ["Speaker of the House"]:
        occupancy.add("constituency", constituency)
    context.emit(occupancy)
    context.emit(person)


def crawl_term(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    pid: str,
    period_start: str | None,
    period_end: str | None,
) -> None:
    # Paginated rosters shift across page boundaries when members are added, so
    # the sitting term is never cached and concluded terms only for same-day retries.
    cache_days = None if period_end is None else 1
    for house in HOUSES:
        query = {"members_search": "1", "keywords": "", "pid": pid, "house": house}
        url: str | None = f"{context.data_url}?{urlencode(query)}"
        total: int | None = None
        rows: list[Element] = []
        while url is not None:
            doc = context.fetch_html(url, cache_days=cache_days)
            if total is None:
                total = int(
                    h.xpath_strings(doc, '//span[@class="result_total"]/text()')[0]
                )
            rows.extend(
                h.xpath_elements(doc, './/div[contains(@class, "sf_result_list")]/a')
            )
            next_urls = h.xpath_strings(doc, './/a[@rel="next"]/@href')
            url = next_urls[0] if next_urls else None
        # A failed page would otherwise end the roster early without an error.
        assert len(rows) == total, (pid, house, len(rows), total)
        for row in rows:
            crawl_member(
                context, position, categorisation, row, period_start, period_end
            )


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Parliament of Trinidad and Tobago",
        country="tt",
        topics=["gov.national", "gov.legislative"],
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    doc = context.fetch_html(context.data_url, cache_days=1)
    for option in h.xpath_elements(doc, '//select[@name="pid"]/option'):
        pid = h.xpath_string(option, "./@value")
        if pid == "All":
            continue
        period_start, period_end = parse_term(h.xpath_string(option, "./text()"))
        if period_end is not None:
            end = h.extract_date(
                context.dataset, period_end, fallback_to_original=False
            )
            if end[0] < h.earliest_term_start(categorisation.topics):
                continue
        crawl_term(context, position, categorisation, pid, period_start, period_end)
