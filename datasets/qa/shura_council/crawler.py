from normality import squash_spaces

from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise
from zavod.util import Element

# No Wikidata item covers either leadership office.
POSITION_QIDS = {"Member of the Shura Council of Qatar": "Q21328600"}

MEMBER_HREF = './/h3//a[contains(@href, "/Members/")]/@href'


def member_cards(doc: Element) -> dict[str, Element]:
    """Map each member's profile path to their card on a roster page.

    The roster ends with an empty card carrying no member link, so cards are keyed
    off the link rather than counted. The path is the same in both locales, which
    is what lets the Arabic and English rosters be joined.
    """
    cards: dict[str, Element] = {}
    for card in h.xpath_elements(doc, '//div[@class="card"]'):
        hrefs = h.xpath_strings(card, MEMBER_HREF)
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
    slug: str,
    en_card: Element,
    ar_card: Element | None,
) -> None:
    role = h.element_text(h.xpath_element(en_card, ".//h4"))
    res = context.lookup("position", role)
    assert res is not None, role

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
    person.add("sourceUrl", h.xpath_string(en_card, MEMBER_HREF))
    # Shura Council members must hold original Qatari nationality (Constitution of
    # Qatar, Article 80(1)). https://www.constituteproject.org/constitution/Qatar_2003
    person.add("citizenship", "qa")

    for title in res.values:
        position = h.make_position(
            context,
            name=title,
            country="qa",
            topics=["gov.national", "gov.legislative"],
            wikidata_id=POSITION_QIDS.get(title),
            lang="eng",
        )
        categorisation = categorise(context, position)
        if not categorisation.is_pep:
            continue
        occupancy = h.make_occupancy(
            context, person, position, categorisation=categorisation
        )
        if occupancy is not None:
            context.emit(occupancy)
            context.emit(position)
            context.emit(person)


def crawl(context: Context) -> None:
    # The English roster carries the official English names and role labels; the
    # Arabic one carries the native-script names. Both are keyed on the same
    # profile path.
    en_cards = member_cards(
        context.fetch_html(context.data_url, cache_days=1, absolute_links=True)
    )
    assert len(en_cards) > 1, len(en_cards)
    ar_cards = member_cards(
        context.fetch_html(
            context.data_url.replace("/en/", "/ar-QA/"),
            cache_days=1,
            absolute_links=True,
        )
    )
    for slug, en_card in en_cards.items():
        crawl_member(context, slug, en_card, ar_cards.get(slug))
