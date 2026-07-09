import re

from lxml.html import HtmlElement

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

MEMBER_RE = re.compile(r"/members/(\d+)")


def crawl_member(
    context: Context,
    row: HtmlElement,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    links = h.xpath_elements(row, ".//a[contains(@href, '/members/')]")
    if not links:
        return
    match = MEMBER_RE.search(links[0].get("href") or "")
    if match is None:
        return
    name = h.element_text(links[0])
    if len(name) == 0:
        return
    cells = h.xpath_elements(row, "./td")
    # Columns: photo, name, constituency, party, membership type.
    constituency = h.element_text(cells[2]) if len(cells) > 2 else None
    party = h.element_text(cells[3]) if len(cells) > 3 else None

    person = context.make("Person")
    person.id = context.make_slug("member", match.group(1))
    h.apply_name(person, full=name, lang="eng")
    person.add("political", party)
    # Members must be citizens of the United Republic of Tanzania and may not hold the
    # citizenship of any other country (Constitution art. 67(1)(a), 67(2)(a)).
    # https://www.constituteproject.org/constitution/Tanzania_2005
    person.add("citizenship", "tz")

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    occupancy.add("constituency", constituency)
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the National Assembly of Tanzania",
        country="tz",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q17599130",
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    doc = context.fetch_html(context.data_url, cache_days=1, absolute_links=True)
    rows = h.xpath_elements(doc, "//tr[td//a[contains(@href, '/members/')]]")
    for row in rows:
        crawl_member(context, row, position, categorisation)
