import re

from lxml.etree import _Element
from normality import squash_spaces
from zavod.stateful.positions import PositionCategorisation, categorise

from zavod import Context, Entity
from zavod import helpers as h
from zavod.extract import zyte_api

# A presiding officer's name carries their office in brackets, e.g.
# "هيبت حمد عباس عبدالجبار الحلبوسي (رئيس مجلس النواب)".
NAME_ROLE = re.compile(r"^(?P<name>[^()]+?)\s*\((?P<role>[^()]+)\)\s*$")


def crawl_presiding_office(context: Context, person: Entity, role: str) -> None:
    res = context.lookup("position", role, warn_unmatched=True)
    if res is None:
        return
    office = h.make_position(
        context,
        name=res.name,
        country="iq",
        topics=res.topics,
        wikidata_id=res.wikidata_id,
        lang="eng",
    )
    categorisation = categorise(context, office)
    if not categorisation.is_pep:
        return
    occupancy = h.make_occupancy(context, person, office, categorisation=categorisation)
    if occupancy is not None:
        context.emit(occupancy)
        context.emit(office)


def crawl_row(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    row: list[_Element],
) -> None:
    _, name_cell, party_cell, governorate_cell = row
    raw_name = h.element_text(name_cell, squash=False)
    role = None
    if "(" in raw_name or ")" in raw_name:
        match = NAME_ROLE.match(raw_name)
        if match is None:
            context.log.warning("Unexpected brackets in member name", name=raw_name)
            return
        # The office is split off so that the ID survives the holder leaving it.
        raw_name, role = match.group("name"), squash_spaces(match.group("role"))
    name = squash_spaces(raw_name)

    person = context.make("Person")
    person.id = context.make_id(raw_name)
    person.add("name", name)
    # Members must be Iraqi: Constitution of Iraq (2005), Art. 49 Second.
    # https://www.constituteproject.org/constitution/Iraq_2005
    person.add("citizenship", "iq")
    party = h.element_text(party_cell)
    # The members holding the nine minority quota seats stand as individuals and
    # have their own name in the party column.
    if party != name:
        person.add("political", party or None)

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    occupancy.add("constituency", h.element_text(governorate_cell) or None)
    context.emit(occupancy)
    if role is not None:
        crawl_presiding_office(context, person, role)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Council of Representatives of Iraq",
        wikidata_id="Q21328603",
        country="iq",
        topics=["gov.legislative", "gov.national"],
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        context.log.warning("Position is not categorised as PEP", position=position.id)
        return
    context.emit(position)

    # Cloudflare serves a JavaScript challenge to anything but a rendering browser.
    doc = zyte_api.fetch_html(context, context.data_url, "//article//table//tr[td]")
    header, *rows = h.xpath_elements(doc, "//article//table//tr")
    # The header is an ordinary row of cells, numbered "1." like the members below.
    header_labels = [h.element_text(td) for td in h.xpath_elements(header, "./td")]
    assert header_labels[1:] == ["اسم النائب", "اسم الحزب", "اسم المحافظة"], (
        header_labels
    )
    for row in rows:
        cells = h.xpath_elements(row, "./td")
        assert len(cells) == 4, h.element_text(row)
        crawl_row(context, position, categorisation, cells)
