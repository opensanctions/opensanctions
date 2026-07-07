from itertools import count

from lxml.html import HtmlElement

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise


def field(row: HtmlElement, name: str) -> str | None:
    cells = h.xpath_elements(row, f".//td[contains(@class, '{name}')]")
    return h.element_text(cells[0]) if cells else None


def crawl_member(
    context: Context,
    row: HtmlElement,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    raw_name = field(row, "views-field-field-name")
    if raw_name is None or len(raw_name) == 0:
        return
    links = h.xpath_elements(row, ".//td[contains(@class, 'view-node')]//a")
    href = links[0].get("href") if links else None
    slug = href.rstrip("/").split("/")[-1] if href is not None else raw_name

    person = context.make("Person")
    person.id = context.make_id(slug)
    clean_name = h.strip_name_titles(context, raw_name)
    original_name = raw_name if clean_name != raw_name else None
    person.add("name", clean_name, lang="eng", original_value=original_name)
    person.add("political", field(row, "views-field-field-party"))
    # Members must be Kenyan citizens (Constitution art. 99); as State officers they
    # may not hold dual citizenship (art. 78(2)).
    # https://www.constituteproject.org/constitution/Kenya_2010
    person.add("citizenship", "ke")

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    # Single-member-constituency MPs carry a constituency; county/woman-rep and
    # nominated members carry only a county.
    occupancy.add("constituency", field(row, "views-field-field-constituency"))
    occupancy.add("constituency", field(row, "views-field-field-county"))
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the National Assembly of Kenya",
        country="ke",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q17510786",
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    for page in count(0):
        doc = context.fetch_html(
            context.data_url, params={"page": page}, cache_days=1, absolute_links=True
        )
        rows = h.xpath_elements(doc, "//tr[contains(@class, 'mp')]")
        if len(rows) == 0:
            break
        for row in rows:
            crawl_member(context, row, position, categorisation)
