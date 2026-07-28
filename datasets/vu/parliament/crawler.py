from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.extract import zyte_api
from zavod.stateful.positions import PositionCategorisation, categorise


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    row: dict[str, str | None],
) -> None:
    raw_name = row.pop("name")
    assert raw_name is not None, "Missing member name"
    # Names carry the honorific "Hon." (declared under `names.prefixes_strip`) and use
    # non-breaking spaces, which strip_name_titles normalises. The surname is written in
    # upper case; we keep the source casing since the matcher normalises it.
    name = h.strip_name_titles(context, raw_name)
    assert name, "Empty member name"
    constituency = row.pop("constituency")

    person = context.make("Person")
    person.id = context.make_id(name, constituency)
    person.add("name", name, original_value=raw_name if name != raw_name else None)
    person.add("political", row.pop("party"))
    # Every citizen of Vanuatu at least 25 years of age is eligible to stand for
    # Parliament (Constitution of Vanuatu, Chapter 4, Article 17(2)).
    # https://www.constituteproject.org/constitution/Vanuatu_2013
    person.add("citizenship", "vu")

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    occupancy.add("constituency", constituency)

    context.audit_data(row, ignore=["position_portfolio", "profile"])
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Parliament of Vanuatu",
        country="vu",
        wikidata_id="Q21294920",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        # The roster table must be present for the fetch to count as unblocked.
        unblock_validator='.//table[contains(@class, "table-striped")]',
        geolocation="au",
        cache_days=14,
    )
    table = h.xpath_element(doc, './/table[contains(@class, "table-striped")]')
    rows = list(h.parse_html_table(table))
    for row in rows:
        crawl_member(context, position, categorisation, h.cells_to_str(row))
