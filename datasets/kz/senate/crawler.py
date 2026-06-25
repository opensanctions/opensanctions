import re

from lxml.html import HtmlElement

from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise


def parse_cards(doc: HtmlElement) -> dict[str, str]:
    """Map each senator's profile id to their full name on a deputies list page."""
    cards: dict[str, str] = {}
    for card in h.xpath_elements(doc, "//a[contains(@class, 'person-card')]"):
        href = card.get("href")
        match = re.search(r"/blog/(\d+)/", href or "")
        if match is None:
            continue
        names = h.xpath_elements(
            card, ".//*[contains(@class, 'person-card--full-name')]"
        )
        cards[match.group(1)] = h.element_text(names[0])
    return cards


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Senate of the Parliament of Kazakhstan",
        country="kz",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21295141",
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    cards = parse_cards(context.fetch_html(context.data_url, cache_days=1))
    for senator_id, name_en in cards.items():
        person = context.make("Person")
        person.id = context.make_slug("senator", senator_id)
        h.apply_name(person, full=name_en, lang="eng")
        # Senators must be citizens of Kazakhstan (Constitution art. 51(4)).
        # https://www.constituteproject.org/constitution/Kazakhstan_2017
        person.add("citizenship", "kz")

        occupancy = h.make_occupancy(
            context, person, position, categorisation=categorisation
        )
        if occupancy is None:
            continue
        context.emit(occupancy)
        context.emit(person)
