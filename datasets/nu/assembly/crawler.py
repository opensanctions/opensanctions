from lxml import html

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    block: html.HtmlElement,
) -> None:
    constituency = h.element_text(h.xpath_element(block, './/p[@class="position"]'))
    # Members carry the honorific "Hon"; strip the affixes declared under
    # `names.prefixes_strip` in the metadata, keeping the raw value as provenance.
    raw_name = h.element_text(h.xpath_element(block, ".//h4"))
    name = h.strip_name_titles(context, raw_name)
    assert name, "Empty member name"
    assert constituency, f"Empty constituency for {name!r}"

    person = context.make("Person")
    person.id = context.make_id(name, constituency)
    person.add("name", name, original_value=raw_name if name != raw_name else None)
    # Members must be a New Zealand citizen or a Permanent Resident of Niue (Constitution
    # of Niue, Article 17(1)(a)) — Niue is self-governing in free association with New
    # Zealand and has no separate citizenship. We therefore record country rather than
    # asserting a specific citizenship. https://faolex.fao.org/docs/pdf/niu132832.pdf
    person.add("country", "nu")

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    occupancy.add("constituency", constituency)

    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Niue Assembly",
        country="nu",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q40011889",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    doc = context.fetch_html(context.data_url, cache_days=14)
    # Select every member card and let crawl_member's xpath_element calls raise if one
    # lacks a name or constituency, rather than filtering incomplete cards out silently.
    blocks = h.xpath_elements(doc, '//div[contains(@class, "cabinet-member")]')
    for block in blocks:
        crawl_member(context, position, categorisation, block)
