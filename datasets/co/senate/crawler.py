from lxml.etree import _Element as Element
from zavod.stateful.positions import PositionCategorisation, categorise

from zavod import Context, Entity
from zavod import helpers as h


def crawl_record(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    record: Element,
) -> None:
    name = h.element_text(h.xpath_element(record, ".//h3"))

    person = context.make("Person")
    person.id = context.make_id(name)
    person.add("name", name)
    person.add(
        "political",
        h.element_text(h.xpath_element(record, ".//h3/following-sibling::p[1]")),
    )

    person.add("sourceUrl", h.xpath_strings(record, ".//blockquote//a/@href"))
    # Senators must be Colombian by birth: Constitución Política de Colombia (1991),
    # Art. 172. http://www.secretariasenado.gov.co/senado/basedoc/constitucion_politica_1991_pr005.html
    person.add("citizenship", "co")

    # The roster lists only the sitting Senate and publishes no dates for it.
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Senate of Colombia",
        wikidata_id="Q19254253",
        country="co",
        topics=["gov.legislative", "gov.national"],
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    doc = context.fetch_html(context.data_url, cache_days=1, absolute_links=True)
    # The page carries the directory twice: a per-letter view, and an all-letters
    # ("Todos") view that holds every senator.
    records = h.xpath_element(
        doc, '//div[@class="modazdirectory__results"][h1="Todos"]'
    )
    for record in h.xpath_elements(
        records, './/div[contains(@class, "modazdirectory__result ")]'
    ):
        crawl_record(context, position, categorisation, record)
