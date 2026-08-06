from normality import squash_spaces

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element

# The roster's own name for plain membership, as opposed to the two leadership
# offices its holders occupy in addition to their seat.
MEMBER = "Member of the Shura Council of Qatar"


def member_cards(doc: Element) -> dict[str, Element]:
    """Map each member's profile path to their card on a roster page.

    The roster ends with an empty card carrying no member link, so cards are keyed
    off the link rather than counted. The path is the same in both locales, which
    is what lets the Arabic and English rosters be joined.
    """
    cards: dict[str, Element] = {}
    for card in h.xpath_elements(doc, '//div[@class="card"]'):
        hrefs = h.xpath_strings(card, './/h3//a[contains(@href, "/Members/")]/@href')
        if len(hrefs) == 0:
            continue
        cards[hrefs[0].split("/Members/")[-1]] = card
    return cards


def card_name(card: Element) -> str:
    """Read the member name out of a card's heading.

    The name sits either inside the leading ``span.h5`` (English roster, with the
    honorific, e.g. ``H.E. Mr. Hassan bin Abdullah Al-Ghanim/``) or as that span's
    tail text (Arabic roster, where the span holds only the honorific). A trailing
    slash separates the two in the source markup and is not part of the name.
    """
    span = h.xpath_element(card, ".//h3//a/span[@class='h5']")
    tail = squash_spaces(span.tail or "")
    name = tail if tail else h.element_text(span)
    return squash_spaces(name.strip("/"))


def crawl_member(
    context: Context,
    positions: dict[str, tuple[Entity, PositionCategorisation]],
    slug: str,
    en_card: Element,
    ar_card: Element | None,
) -> None:
    role = h.element_text(h.xpath_element(en_card, ".//h4"))
    title = context.lookup_value("position", role)
    assert title is not None, role

    raw_name = card_name(en_card)
    name = h.strip_name_titles(context, raw_name)
    if name is None:
        return

    person = context.make("Person")
    # The profile path carries the member's name, so hash it rather than slugging it.
    person.id = context.make_id(slug)
    person.add(
        "name", name, lang="eng", original_value=raw_name if name != raw_name else None
    )
    if ar_card is not None:
        person.add("name", card_name(ar_card), lang="ara")
    person.add(
        "sourceUrl",
        f"https://www.shura.qa/en/Pages/About-Council/President-and-Members/Members/{slug}",
    )
    # Shura Council members must hold original Qatari nationality (Constitution of
    # Qatar, Article 80(1)). https://www.constituteproject.org/constitution/Qatar_2003
    person.add("citizenship", "qa")

    # The Speaker and Deputy Speaker are elected from among the members and hold
    # their seat as well, so they get an occupancy for each office.
    held = [MEMBER] if title == MEMBER else [MEMBER, title]
    emitted = False
    for held_title in held:
        position, categorisation = positions[held_title]
        occupancy = h.make_occupancy(
            context, person, position, categorisation=categorisation
        )
        if occupancy is None:
            continue
        context.emit(occupancy)
        emitted = True
    if emitted:
        context.emit(person)


def crawl(context: Context) -> None:
    positions: dict[str, tuple[Entity, PositionCategorisation]] = {}
    for title, wikidata_id in (
        (MEMBER, "Q21328600"),
        # No Wikidata item covers either leadership office, so these are keyed on
        # their name instead.
        ("Speaker of the Shura Council of Qatar", None),
        ("Deputy Speaker of the Shura Council of Qatar", None),
    ):
        position = h.make_position(
            context,
            name=title,
            country="qa",
            topics=["gov.national", "gov.legislative"],
            wikidata_id=wikidata_id,
            lang="eng",
        )
        categorisation = categorise(context, position)
        if not categorisation.is_pep:
            continue
        context.emit(position)
        positions[title] = (position, categorisation)

    if MEMBER not in positions:
        return

    # The English roster carries the official English names and role labels; the
    # Arabic one carries the native-script names. Both are keyed on the same
    # profile path.
    en_cards = member_cards(context.fetch_html(context.data_url, cache_days=1))
    assert len(en_cards) > 1, len(en_cards)
    ar_cards = member_cards(
        context.fetch_html(
            context.data_url.replace("/en/", "/ar-QA/"),
            cache_days=1,
        )
    )
    for slug, en_card in en_cards.items():
        crawl_member(context, positions, slug, en_card, ar_cards.get(slug))
