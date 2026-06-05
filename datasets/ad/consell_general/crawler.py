from normality import squash_spaces

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element

# The roster lists 28 members; the first tile on the page is the section's intro
# (a "Consellers i conselleres generals" heading), not a person.
INTRO_SLUG = "consellers-generals-1"


def field(doc: Element, css_class: str) -> str | None:
    """Return the text value of a labelled profile field, e.g. ``div.datanaixement``.

    Each member profile renders its attributes as ``<div class="…"><b>Label</b>
    <span>value</span></div>`` blocks with stable class names. Returns None when the
    block is absent (not every member fills in every field).
    """
    blocks = h.xpath_elements(doc, f"//div[@class='{css_class}']")
    if not blocks:
        return None
    spans = h.xpath_elements(blocks[0], ".//span")
    if not spans:
        return None
    value = squash_spaces(h.element_text(spans[0]))
    return value or None


def crawl_member(
    context: Context,
    url: str,
    name: str,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    doc = context.fetch_html(url, cache_days=30)

    person = context.make("Person")
    # The profile slug is the source's per-member key; hash it so the name (PII) does
    # not leak into the entity id.
    person.id = context.make_id(url)
    # Names are kept as published (given-name-first, Catalan diacritics).
    person.add("name", name, lang="cat")
    h.apply_date(person, "birthDate", field(doc, "datanaixement"))
    # Members must be Andorran nationals: Constitution of Andorra, Art. 25.
    # https://www.constituteproject.org/constitution/Andorra_1993
    person.add("citizenship", "ad")
    group = field(doc, "grup")
    person.add("political", group, lang="cat")

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        start_date=field(doc, "dataeleccio"),
        no_end_implies_current=True,
    )
    if occupancy is None:
        return
    occupancy.add("politicalGroup", group, lang="cat")
    occupancy.add("constituency", field(doc, "electe"), lang="cat")
    occupancy.add("sourceUrl", url)
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the General Council of Andorra",
        country="ad",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q20177831",
    )
    context.emit(position)
    categorisation = categorise(context, position, default_is_pep=True)

    doc = context.fetch_html(context.data_url, absolute_links=True, cache_days=1)
    headlines = h.xpath_elements(
        doc, "//div[contains(@class, 'tileItem')]//h3[@class='tileHeadline']/a"
    )
    for link in headlines:
        url = link.get("href")
        if url is None or url.rstrip("/").endswith(INTRO_SLUG):
            continue
        name = squash_spaces(h.element_text(link))
        crawl_member(context, url, name, position, categorisation)
