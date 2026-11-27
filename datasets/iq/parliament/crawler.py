import re

from lxml.etree import _Element
from normality import squash_spaces
from zavod.shed.trans import apply_translit_full_name
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import LangText

from zavod import Context, Entity
from zavod import helpers as h
from zavod.extract import zyte_api

# Presiding members carry their role in brackets: "<name> (رئيس مجلس النواب)".
NAME_ROLE = re.compile(r"^(?P<name>[^()]+?)\s*\((?P<role>[^()]+)\)\s*$")
TOPICS = ["gov.national", "gov.legislative"]


def crawl_presiding_role(context: Context, person: Entity, role: str) -> None:
    # Held in addition to the seat.
    title = context.lookup_value("presiding_role", role, warn_unmatched=True)
    if title is None:
        return
    presiding_position = h.make_position(
        context,
        name=f"{title} of the Council of Representatives of Iraq",
        country="iq",
        topics=TOPICS,
        # The deputy roles have no Wikidata item.
        wikidata_id="Q20404109" if title == "Speaker" else None,
        lang="eng",
    )
    categorisation = categorise(context, presiding_position)
    if not categorisation.is_pep:
        return
    occupancy = h.make_occupancy(
        context, person, presiding_position, categorisation=categorisation
    )
    if occupancy is not None:
        context.emit(occupancy)
        context.emit(presiding_position)


def crawl_row(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    row: dict[str, _Element],
) -> None:
    raw_name = h.element_text(row.pop("name"), squash=False)
    party = h.element_text(row.pop("party"))
    governorate = h.element_text(row.pop("governorate"))
    context.audit_data(row, ignore=["row_number"])

    role = None
    if "(" in raw_name or ")" in raw_name:
        match = NAME_ROLE.match(raw_name)
        if match is None:
            context.log.warning("Unexpected brackets in member name", name=raw_name)
            return
        # Keeps the ID stable when the holder leaves office.
        raw_name, role = match.group("name"), squash_spaces(match.group("role"))
    name = squash_spaces(raw_name)

    person = context.make("Person")
    person.id = context.make_id(raw_name)
    person.add("name", name)
    apply_translit_full_name(context, person, LangText(name, "ara"))
    person.add("citizenship", "iq")
    # Minority quota members list their own name as the party.
    if party != name:
        person.add("political", party)

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    occupancy.add("constituency", governorate)
    context.emit(occupancy)
    if role is not None:
        crawl_presiding_role(context, person, role)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Council of Representatives of Iraq",
        wikidata_id="Q21328603",
        country="iq",
        topics=TOPICS,
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        context.log.warning("Position is not categorised as PEP", position=position.id)
        return
    context.emit(position)

    # Behind a Cloudflare JavaScript challenge.
    doc = zyte_api.fetch_html(context, context.data_url, "//article//table//tr[td]")
    table = h.xpath_element(doc, "//article//table")
    for cells in h.parse_html_table(table, header_tag="td", slugify_headers=False):
        row: dict[str, _Element] = {}
        for header, cell in cells.items():
            column = context.lookup_value("columns", header)
            assert column is not None, header
            row[column] = cell
        crawl_row(context, position, categorisation, row)
