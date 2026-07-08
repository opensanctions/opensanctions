import re
from itertools import count

from lxml.html import HtmlElement

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

MP_RE = re.compile(r"mp=(\d+)")


def crawl_member(
    context: Context,
    card: HtmlElement,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    match = MP_RE.search(card.get("href") or "")
    if match is None:
        return
    names = h.xpath_elements(card, ".//h5")
    name = h.element_text(names[0]) if names else None
    if name is None or len(name) == 0:
        return

    person = context.make("Person")
    person.id = context.make_slug("mp", match.group(1))
    h.apply_name(person, full=name)
    # Members of Parliament must be citizens of Ghana (Constitution art. 94(1)(a)) and
    # may not owe allegiance to another country (art. 94(2)(a)).
    # https://www.constituteproject.org/constitution/Ghana_1996
    person.add("citizenship", "gh")

    # The card's <p> holds "<constituency><br><party>".
    paragraphs = h.xpath_elements(card, ".//p")
    constituency = party = None
    if paragraphs:
        constituency = (paragraphs[0].text or "").strip() or None
        breaks = h.xpath_elements(paragraphs[0], ".//br")
        if breaks and breaks[0].tail is not None:
            party = breaks[0].tail.strip() or None
    person.add("political", party)

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
        name="Member of the Parliament of Ghana",
        country="gh",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21290881",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    seen: set[str] = set()
    for page in count(1):
        doc = context.fetch_html(
            context.data_url, params={"page": page}, cache_days=1, absolute_links=True
        )
        cards = h.xpath_elements(doc, "//a[contains(@href, 'members?mp=')]")
        fresh = 0
        for card in cards:
            match = MP_RE.search(card.get("href") or "")
            if match is None or match.group(1) in seen:
                continue
            seen.add(match.group(1))
            fresh += 1
            crawl_member(context, card, position, categorisation)
        if fresh == 0:
            break
