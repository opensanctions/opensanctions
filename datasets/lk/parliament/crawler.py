from itertools import count

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element

# The directory serves eight members per page by default; the page size selector
# offers 64, which fetches the whole roster in four requests instead of twenty-nine.
PAGE_SIZE = 64


def labelled_value(el: Element, label: str) -> str | None:
    """Return the text of the <p> following a <p><b>{label}</b></p> block."""
    labels = h.xpath_elements(el, f".//p[b[normalize-space()='{label}']]")
    if len(labels) == 0:
        return None
    sibling = labels[0].getnext()
    return h.element_text(sibling) if sibling is not None else None


def crawl_member(
    context: Context,
    card: Element,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    hrefs = h.xpath_strings(card, ".//a[contains(@href, 'mp-profile/')]/@href")
    if len(hrefs) == 0:
        return
    href = hrefs[0]
    # The profile path ends in a numeric member id, which is what the person is keyed on.
    slug = href.rstrip("/").split("/")[-1]

    name_els = h.xpath_elements(card, ".//div[contains(@class, 'mp_name_div')]/p[1]/b")
    if len(name_els) == 0:
        return
    raw_name = h.element_text(name_els[0])

    person = context.make("Person")
    person.id = context.make_slug(slug)
    clean_name = h.strip_name_titles(context, raw_name)
    original_name = raw_name if clean_name != raw_name else None
    person.add("name", clean_name, lang="eng", original_value=original_name)
    person.add("political", labelled_value(card, "Political Party"))
    # MPs must be citizens of Sri Lanka (Constitution Art. 90 read with Art. 89(a));
    # dual citizens are barred (Art. 91(1)(d)(xiii), reinstated by the 21st Amendment).
    # https://www.parliament.lk/files/pdf/constitution.pdf
    person.add("citizenship", "lk")

    profile = context.fetch_html(href, cache_days=7)
    # The per-member profile page carries a clean ISO date of birth.
    h.apply_date(person, "birthDate", labelled_value(profile, "Date of Birth"))
    # Only members holding a ministerial office have a portfolio. It is the office
    # that carries the political exposure, so it is worth recording even though the
    # crawler models a single Position for the seat itself.
    person.add("position", labelled_value(profile, "Portfolio"), lang="eng")
    # A parliament.lk mailbox, not a private address.
    person.add("email", labelled_value(profile, "Email"))
    person.add("sourceUrl", href)
    # Deliberately skipped: the profile also publishes each member's home address and
    # telephone numbers, which we do not collect.

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    occupancy.add("constituency", labelled_value(card, "District"))
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Parliament of Sri Lanka",
        country="lk",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21294918",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    for page in count(1):
        doc = context.fetch_html(
            context.data_url,
            params={"page": page, "itemCount": PAGE_SIZE},
            cache_days=1,
            absolute_links=True,
        )
        cards = h.xpath_elements(doc, "//div[contains(@class, 'overlap_mt_30')]")
        # The first page past the end renders no cards, which ends the pagination.
        if len(cards) == 0:
            break
        for card in cards:
            crawl_member(context, card, position, categorisation)
