from datetime import date
from itertools import count

from zavod import Context, Entity
from zavod import helpers as h
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element

FIELD = (
    './/div[contains(concat(" ", normalize-space(@class), " "), " field-name-{} ")]'
    '/div[@class="field-items"]'
    '/div[contains(concat(" ", normalize-space(@class), " "), " field-item ")]'
)


def field_values(doc: Element, name: str) -> list[str]:
    """Return the values of a profile field, e.g. `field-profession`."""
    return [h.element_text(el) for el in h.xpath_elements(doc, FIELD.format(name))]


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    url: str,
) -> None:
    node_id = url.rsplit("/", 1)[-1]
    assert node_id.isdigit(), url
    doc = context.fetch_html(url, cache_days=7)

    person = context.make("Person")
    person.id = context.make_slug(node_id)
    person.add("name", h.element_text(h.xpath_element(doc, './/h1[@id="page-title"]')))
    person.add("sourceUrl", url)
    # Art. 70(1)(a) of the Constitution requires every Member of Parliament to be a
    # citizen, Art. 69(2) applies the same test to presidentially nominated members,
    # and Art. 72(2)(f) and (3)(d) vacate the seat on loss of citizenship. Members may
    # hold dual citizenship; only the President and the Speaker may not.
    # https://www.constituteproject.org/constitution/Zambia_2016
    person.add("citizenship", "zm")
    person.add("profession", field_values(doc, "field-profession"))
    person.add("education", field_values(doc, "field-educational-qualification"))
    person.add("education", field_values(doc, "field-educational-awards"))
    person.add("education", field_values(doc, "field-professional-training"))
    date_span = (
        'span[contains(concat(" ", normalize-space(@class), " "), " date-display-single ")]'
    )
    born = h.xpath_strings(doc, f"{FIELD.format('field-date')}/{date_span}/@content")
    h.apply_dates(person, "birthDate", [d[:10] for d in born])

    occupancy = h.make_occupancy(
        context,
        person,
        position,        
        categorisation=categorisation,
    )

    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the National Assembly of Zambia",
        wikidata_id="Q18607856",
        country="zm",
        topics=["gov.national", "gov.legislative"],
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    listing_links = (
        './/div[contains(concat(" ", normalize-space(@class), " "),'
        ' " view-id-members_of_parliament ")]'
        '//span[contains(concat(" ", normalize-space(@class), " "), " views-field-title ")]'
        "//a/@href"
    )
    for page in count(0):
        doc = context.fetch_html(
            context.data_url,
            params={"page": page},
            cache_days=1,
            absolute_links=True,
        )
        urls = h.xpath_strings(doc, listing_links)
        # The pager renders an empty view past the last page.
        if len(urls) == 0:
            break
        for url in urls:
            crawl_member(context, position, categorisation, url)
