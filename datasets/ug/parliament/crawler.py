import re
from itertools import count

from lxml.html import HtmlElement

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# The party acronym is embedded in the photo filename as the last "(...)" token.
PARTY_RE = re.compile(r"\(([^()]+)\)[^()]*$")


def crawl_member(
    context: Context,
    card: HtmlElement,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    headings = h.xpath_elements(card, ".//h5")
    if not headings:
        return
    # Names are prefixed "Hon.".
    name = h.element_text(headings[0]).removeprefix("Hon.").strip()
    if len(name) == 0:
        return
    # Two location spans: the constituency/seat designation and the district.
    spans = [h.element_text(s) for s in h.xpath_elements(card, ".//span")]
    locations = [s for s in spans[:2] if len(s) > 0]
    district = locations[-1] if locations else None

    person = context.make("Person")
    # The profile-link tokens rotate between runs, so key on name + district instead.
    person.id = context.make_id(name, district)
    h.apply_name(person, full=name, lang="eng")
    images = h.xpath_elements(card, ".//img")
    if images:
        match = PARTY_RE.search(images[0].get("src") or "")
        if match is not None:
            person.add("political", match.group(1))
    # Members of Parliament must be citizens of Uganda (Constitution art. 80(1)(a)).
    # https://www.constituteproject.org/constitution/Uganda_2017
    person.add("citizenship", "ug")

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    for location in locations:
        occupancy.add("constituency", location)
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Parliament of Uganda",
        country="ug",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21296005",
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    seen: set[str] = set()
    for page in count(1):
        doc = context.fetch_html(context.data_url, params={"page": page}, cache_days=1)
        cards = h.xpath_elements(doc, "//a[contains(@href, '/home/mp/')]")
        fresh = 0
        for card in cards:
            href = card.get("href") or ""
            if href in seen:
                continue
            seen.add(href)
            fresh += 1
            crawl_member(context, card, position, categorisation)
        if fresh == 0:
            break
