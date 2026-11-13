from collections.abc import Iterator
from urllib.parse import urljoin

from zavod import Context, Entity
from zavod import helpers as h
from zavod.stateful.positions import (
    OccupancyStatus,
    PositionCategorisation,
    categorise,
)
from zavod.util import Element

FIELD = (
    './/div[contains(@class, "field-name-{}")]'
    '/div[contains(@class, "field-items")]'
    '/div[contains(@class, "field-item")]'
)


def format_field_values(doc: Element, name: str) -> list[str]:
    """Return the values of a profile field, e.g. `field-profession`."""
    return [h.element_text(el) for el in h.xpath_elements(doc, FIELD.format(name))]


def iter_listing(context: Context, url: str) -> Iterator[tuple[str, str]]:
    """Yield the (URL, title) links of a paginated listing view, e.g. members or
    constituencies."""
    doc = context.fetch_html(url, cache_days=1, absolute_links=True)
    # The "last »" pager link reads `…?page=N`, with pages numbered from 0.
    last_url = h.xpath_string(doc, './/li[contains(@class, "pager-last")]/a/@href')
    last_page = int(last_url.split("?page=")[1])
    for page in range(last_page + 1):
        if page > 0:
            doc = context.fetch_html(
                url, params={"page": page}, cache_days=1, absolute_links=True
            )
        # Titles are a <span> in the members listing and a <div> in the
        # constituencies one.
        anchors = h.xpath_elements(
            doc,
            './/div[contains(@class, "view-id-members_of_parliament")]'
            '//*[contains(@class, "views-field-title")]//a',
        )
        assert len(anchors) > 0, (url, page)
        for anchor in anchors:
            href = anchor.get("href")
            assert href is not None, (url, page)
            yield href, h.element_text(anchor)


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    url: str,
    sitting_start: str | None,
) -> tuple[str, str]:
    """Emit a member and return the start date and constituency of their newest
    term. With `sitting_start`, skip the member if that term began before it."""
    node_id = url.rsplit("/", 1)[-1]
    assert node_id.isdigit(), url
    doc = context.fetch_html(url, cache_days=7)

    person = context.make("Person")
    person.id = context.make_slug(node_id)
    person.add("name", h.element_text(h.xpath_element(doc, './/h1[@id="page-title"]')))
    person.add("sourceUrl", url)
    # Art. 70(1)(a) of the Constitution requires every Member of Parliament to be a
    # citizen, Art. 69(2) applies the same test to presidentially nominated members,
    # and Art. 72(2)(f) and (3)(d) vacate the seat on loss of citizenship. Members may
    # hold dual citizenship; only the President and the Speaker may not.
    # https://www.constituteproject.org/constitution/Zambia_2016
    person.add("citizenship", "zm")
    person.add("profession", format_field_values(doc, "field-profession"))
    person.add("education", format_field_values(doc, "field-educational-qualification"))
    person.add("education", format_field_values(doc, "field-educational-awards"))
    person.add("education", format_field_values(doc, "field-professional-training"))
    # The displayed date reads "23rd July 1959"; `content` holds it as ISO 8601.
    born = h.xpath_strings(doc, FIELD.format("field-date") + "//@content")
    h.apply_dates(person, "birthDate", [d[:10] for d in born])

    # Each "Parliamentary History" row reads "<constituency> - <start> - <party>",
    # newest first. The newest row is the sitting term; earlier rows have ended,
    # but the source gives no end date for them.
    terms: list[tuple[str, str, str]] = []
    for cell in h.xpath_elements(
        doc,
        './/div[@id="block-views-members-of-parliament-block-1"]'
        '//div[contains(@class, "views-row")]//div[contains(@class, "field-content")]',
    ):
        # Some profiles render a blank history row.
        if h.element_text(cell) == "":
            continue
        span = h.xpath_element(cell, './span[contains(@class, "date-display-single")]')
        start = span.get("content")
        head = (cell.text or "").strip()
        tail = (span.tail or "").strip()
        constituency = head.removesuffix("-").strip()
        party = tail.removeprefix("-").strip()
        if (
            start is None
            or not head.endswith("-")
            or not tail.startswith("-")
            or constituency == ""
        ):
            raise ValueError(
                f"Unexpected history row: {h.element_text(cell)!r} ({url})"
            )
        terms.append((start[:10], constituency, party))
    if len(terms) == 0:
        raise ValueError(f"No parliamentary history: {url}")
    starts = [start for start, _, _ in terms]
    if starts != sorted(set(starts), reverse=True):
        raise ValueError(f"History not in descending date order: {starts} ({url})")
    if sitting_start is not None and starts[0] < sitting_start:
        context.log.info("Skipping former member", url=url, start=starts[0])
        return terms[0][:2]

    for idx, (start, constituency, party) in enumerate(terms):
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            start_date=start,
            categorisation=categorisation,
            # Earlier terms have no end date, so would otherwise resolve to `current`.
            status=None if idx == 0 else OccupancyStatus.ENDED,
        )
        # Only the current term can be dropped, since earlier ones have an explicit
        # status; a member whose current term is dropped is not a PEP.
        if occupancy is None:
            return terms[0][:2]
        constituency_res = context.lookup("constituency", constituency)
        occupancy.add(
            "constituency", constituency_res.value if constituency_res else constituency
        )
        if party != "":
            occupancy.add(
                "politicalGroup",
                context.lookup_value("political_group", party, warn_unmatched=True),
            )
        context.emit(occupancy)
    context.emit(person)
    return terms[0][:2]


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the National Assembly of Zambia",
        wikidata_id="Q18607856",
        country="zm",
        topics=["gov.national", "gov.legislative"],
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    # The members listing is the source's roster of the sitting Assembly, but it
    # omits some members who are linked from their constituency page.
    members_url = [url for url, _ in iter_listing(context, context.data_url)]
    newest = [
        crawl_member(context, position, categorisation, url, None)
        for url in members_url
    ]
    # Constituency pages also link members of earlier terms whose seat has not been
    # updated, so a member found only there is emitted only if their newest term
    # began no earlier than any listed member's.
    sitting_start = min(start for start, _ in newest)
    # Only a seat no listed member holds can have a sitting member missing from
    # the listing.
    held = {constituency for _, constituency in newest}
    seen = set(members_url)
    constituencies_url = urljoin(context.data_url, "/members/constituencies")
    for constituency_url, name in iter_listing(context, constituencies_url):
        if name in held:
            continue
        doc = context.fetch_html(constituency_url, cache_days=1, absolute_links=True)
        for url in h.xpath_strings(
            doc,
            './/div[contains(@class, "pane-members-of-parliament")]'
            '//div[contains(@class, "views-field-view-node")]//a/@href',
        ):
            if url in seen:
                continue
            seen.add(url)
            crawl_member(context, position, categorisation, url, sitting_start)
