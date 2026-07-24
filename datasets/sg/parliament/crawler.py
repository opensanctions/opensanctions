from urllib.parse import urljoin

from lxml import html

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    card: html.HtmlElement,
) -> None:
    # `mp-last-name` carries the full name without the honorific title.
    name = h.element_text(
        h.xpath_element(card, './/*[contains(@class, "mp-last-name")]')
    )
    assert name, "Empty MP name"
    link = h.xpath_element(card, './/*[contains(@class, "mp-sort-name")]//a')
    href = link.get("href")
    assert href is not None, f"Missing detail link for {name}"
    slug = href.rstrip("/").split("/")[-1]

    constituencies = h.xpath_strings(
        card, './/*[contains(@class, "constituency")]/text()'
    )
    constituency = " ".join(" ".join(constituencies).split()) or None

    person = context.make("Person")
    person.id = context.make_slug(slug)
    person.add("name", name)
    person.add("sourceUrl", urljoin(context.data_url, href))
    # A Member of Parliament must be a citizen of Singapore (Constitution of the Republic
    # of Singapore, Article 44(2)(a)). https://sso.agc.gov.sg/Act/CONS1963?ProvIds=pr44-
    person.add("citizenship", "sg")

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
        name="Member of the Parliament of Singapore",
        country="sg",
        wikidata_id="Q21294917",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    doc = context.fetch_html(context.data_url, cache_days=1)
    cards = h.xpath_elements(
        doc,
        '//div[contains(@class, "list-of-mps-wrap")][.//*[contains(@class, "mp-last-name")]]',
    )
    if not cards:
        raise ValueError("No MP cards found on the Singapore list page")
    for card in cards:
        crawl_member(context, position, categorisation, card)
