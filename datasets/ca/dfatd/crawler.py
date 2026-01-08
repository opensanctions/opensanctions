from typing import Dict, List
from lxml.etree import _Element
from normality import squash_spaces
from rigour.mime.types import XML
from followthemoney.types import registry

from zavod import Context
from zavod import helpers as h

NAME_SPLITS = [
    " (a.k.a.",
    " (also known as",
    "/",
]
ALIAS_SPLITS = [
    ", ",
    "; ",
    " (a.k.a.",
    " (also known as",
    "; a.k.a. ",
    "ALIAS: ",
    "Hebrew: ",
    "Arabic: ",
    "Belarusian:",
    "Belarussian:",
    "Russian:",
    "Ukrainian:",
]


def split_name(name: str) -> List[str]:
    name = squash_spaces(name)
    parts: List[str] = []
    for part in h.multi_split(name, NAME_SPLITS):
        part = part.rstrip(")").rstrip(";")
        if len(part):
            parts.append(part)
    return parts


def crawl(context: Context):
    path = context.fetch_resource("source.xml", context.data_url)
    context.export_resource(path, XML, title=context.SOURCE_TITLE)
    doc = context.parse_resource_xml(path)
    for node in doc.findall(".//record"):
        parse_entry(context, node)


def parse_entry(context: Context, node: _Element):
    row: Dict[str, str] = {}
    for child in node:
        if child.text is not None:
            row[child.tag] = child.text

    entity_name = row.pop("EntityOrShip", None)
    given_name = row.pop("GivenName", None)
    last_name = row.pop("LastName", None)
    dob = row.pop("DateOfBirthOrShipBuildDate", None)
    title = row.pop("TitleOrShip", None)
    imo_number = row.pop("ShipIMONumber", None)
    schedule = row.pop("Schedule", None)
    if schedule in ("N/A", None):
        schedule = ""
    if entity_name is None:
        entity_name = h.make_name(given_name=given_name, last_name=last_name)
    program = row.pop("Country")
    country = program
    if program is not None and "/" in program:
        country, _ = program.split("/", 1)

    entity = context.make("LegalEntity")
    country_code = registry.country.clean(country)
    entity.id = context.make_id(schedule, country_code, entity_name)
    if imo_number is not None:
        entity = context.make("Vessel")
        entity.id = context.make_id(schedule, country_code, entity_name, imo_number)
        entity.add("imoNumber", imo_number)
        if entity_name is not None:
            entity.add("name", squash_spaces(entity_name))
        entity.add("type", title)
        h.apply_date(entity, "buildDate", dob)
    elif given_name is not None or last_name is not None or dob is not None:
        entity.add_schema("Person")
        h.apply_name(entity, first_name=given_name, last_name=last_name)
        h.apply_date(entity, "birthDate", dob)
        entity.add("title", title)
    elif entity_name is not None:
        entity.add("name", split_name(entity_name))
        h.apply_date(entity, "incorporationDate", dob)
        assert dob is None, (dob, entity_name)

    entity.add("topics", "sanction")
    entity.add("country", country)
    program_key = h.lookup_sanction_program_key(context, program) if program else None
    sanction = h.make_sanction(
        context,
        entity,
        program_name=program,
        source_program_key=program,
        program_key=program_key,
    )
    sanction.add("program", program)
    sanction.add("reason", schedule)
    sanction.add("authorityId", row.pop("Item"))
    h.apply_date(sanction, "listingDate", row.pop("DateOfListing", None))

    names = squash_spaces(row.pop("Aliases", ""))
    for name in h.multi_split(names, ALIAS_SPLITS):
        trim_name = squash_spaces(name)
        # if " or " in trim_name:
        #     print("ALIAS", trim_name)
        entity.add("alias", trim_name)

    context.audit_data(row)
    context.emit(entity)
    context.emit(sanction)
