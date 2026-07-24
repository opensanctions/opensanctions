from itertools import count

from lxml.html import HtmlElement

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise


def labelled_value(el: HtmlElement, label: str) -> str | None:
    """Return the text of the <p> following a <p><b>{label}</b></p> block."""
    labels = h.xpath_elements(el, f".//p[b[normalize-space()='{label}']]")
    if len(labels) == 0:
        return None
    sibling = labels[0].getnext()
    return h.element_text(sibling) if sibling is not None else None


def crawl_member(
    context: Context,
    card: HtmlElement,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    links = h.xpath_elements(card, ".//a[contains(@href, 'mp-profile/')]")
    href = links[0].get("href")
    if href is None:
        return
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

    # The per-member profile page carries a clean ISO date of birth.
    profile = context.fetch_html(href, cache_days=7)
    dob = labelled_value(profile, "Date of Birth")
    h.apply_date(person, "birthDate", dob)
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
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    seen = False
    for page in count(1):
        doc = context.fetch_html(
            context.data_url,
            params={"page": page},
            cache_days=1,
            absolute_links=True,
        )
        cards = h.xpath_elements(doc, "//div[contains(@class, 'overlap_mt_30')]")
        if len(cards) == 0:
            break
        seen = True
        for card in cards:
            crawl_member(context, card, position, categorisation)
    if not seen:
        raise ValueError("No member cards found — directory layout may have changed.")
