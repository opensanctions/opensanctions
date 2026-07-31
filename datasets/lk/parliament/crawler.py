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
    siblings = h.xpath_elements(
        el, f".//p[b[normalize-space()='{label}']]/following-sibling::p[1]"
    )
    return h.element_text(siblings[0]) if siblings else None


def crawl_member(
    context: Context,
    card: Element,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    href = h.xpath_string(card, ".//a[contains(@href, 'mp-profile/')]/@href")
    # The profile path ends in a numeric member id, which is what the person is keyed on.
    slug = href.rstrip("/").split("/")[-1]
    raw_name = h.xpath_string(
        card, ".//div[contains(@class, 'mp_name_div')]/p[1]/b/text()"
    )

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
    h.apply_date(person, "birthDate", labelled_value(profile, "Date of Birth"))
    person.add("position", labelled_value(profile, "Portfolio"), lang="eng")
    person.add("email", labelled_value(profile, "Email"))
    person.add("sourceUrl", href)

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
            cache_days=7,
            absolute_links=True,
        )
        cards = h.xpath_elements(doc, "//div[contains(@class, 'overlap_mt_30')]")
        # The first page past the end renders no cards, which ends the pagination.
        if len(cards) == 0:
            break
        for card in cards:
            crawl_member(context, card, position, categorisation)
