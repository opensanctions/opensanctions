from lxml.etree import _Element

from zavod import Context, Entity
from zavod import helpers as h
from zavod.stateful.positions import PositionCategorisation, categorise


def crawl_member(
    context: Context,
    card: _Element,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    url = card.get("href")
    if url is None:
        raise ValueError("Member card without a link")

    person = context.make("Person")
    # Slugs are carried over from the former REST API pages, so IDs stay stable.
    person.id = context.make_slug(url.rstrip("/").rsplit("/", 1)[-1])
    h.apply_name(
        person,
        first_name=h.element_text(
            h.xpath_element(card, ".//span[@class='legislature-member__firstname']")
        ),
        last_name=h.element_text(
            h.xpath_element(card, ".//span[@class='legislature-member__lastname']")
        ),
    )
    person.add("sourceUrl", url)
    # National Council members must have held Monegasque nationality for at least five
    # years (Constitution of Monaco, Article 54).
    # https://www.constituteproject.org/constitution/Monaco_2002
    person.add("citizenship", "mc")

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the National Council of Monaco",
        country="mc",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21328626",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    # The REST API now requires authentication; the members page lists the sitting
    # councillors grouped by political group.
    doc = context.fetch_html(context.data_url, cache_days=1)
    cards = h.xpath_elements(doc, "//a[@class='legislature-member__card']")
    for card in cards:
        crawl_member(context, card, position, categorisation)
