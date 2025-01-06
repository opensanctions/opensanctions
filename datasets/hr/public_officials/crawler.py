import csv
from typing import Any, List, Optional

from rigour.mime.types import CSV

from zavod import Context, Entity
from zavod import helpers as h
from zavod.logic.pep import categorise


EXPECTED_COLUMNS_OBLIGATED = [
    "Ime",
    "Prezime",
    "Primarna dužnost",
    "Pravna osoba u kojoj obnaša dužnost",
    "Datum početka obnašanja dužnosti",
    "Datum kraja obnašanja dužnosti",
    "Sekundarna dužnost",
    "Pravna osoba u kojoj obnaša dužnost",
    "Datum početka obnašanja dužnosti",
    "Datum kraja obnašanja dužnosti",
]

EXPECTED_COLUMNS_CIVIL_SERVANTS = [
    "Ime",
    "Prezime",
    "Dužnost",
    "Pravna osoba u kojoj obnaša dužnost",
    "Datum početka obnašanja dužnosti",
    "Datum kraja obnašanja dužnosti",
    "Dužnost",
    "Pravna osoba u kojoj obnaša dužnost",
    "Datum početka obnašanja dužnosti",
    "Datum kraja obnašanja dužnosti",
]

# Define readable column names for appointed civil servants
FIELDS_CIVIL_SERVANTS = [
    "First Name",
    "Last Name",
    "Primary Position",
    "Primary Legal Entity",
    "Primary Position Start Date",
    "Primary Position End Date",
    "Secondary Position",
    "Secondary Legal Entity",
    "Secondary Position Start Date",
    "Secondary Position End Date",
]

# Define readable column names for obligors
FIELDS_OBLIGATED_PERSONS = [
    "First Name",
    "Last Name",
    "Primary Position",
    "Primary Legal Entity",
    "Primary Position Start Date",
    "Primary Position End Date",
    "Secondary Position",
    "Secondary Legal Entity",
    "Secondary Position Start Date",
    "Secondary Position End Date",
]


def make_position_name(data: dict) -> Optional[str]:
    title = data.pop("Position")
    legal_entity_name = data.pop("Legal Entity")
    if not any([title, legal_entity_name]):
        return None

    position_name = title or "Unknown position"
    if legal_entity_name:
        position_name = f"{position_name}, {legal_entity_name}"
    return position_name


def make_affiliation_entities(
    context: Context,
    person: Entity,
    position_name: str,
    data: dict,
    position_topics: List[str],
) -> List[Entity]:
    """Creates Position and Occupancy provided that the Occupancy meets OpenSanctions criteria.
    * A position's name include the title and optionally the name of the legal entity
    * A position with a legal entity but no title is titled 'Unknown position'
    * All positions (and Occupancies, Persons) are assumed to be Croatian
    * Positions with start and/or end date but no position name or legal entity name are discarded
    """
    if position_name is None or position_name.strip() == "":
        return []

    start_date = data.pop("Position Start Date")
    end_date = data.pop("Position End Date")
    context.audit_data(data)

    position = h.make_position(
        context, position_name, topics=position_topics, country="HR"
    )

    categorisation = categorise(context, position, is_pep=True)
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        no_end_implies_current=True,
        categorisation=categorisation,
        propagate_country=True,
        start_date=start_date,
        end_date=end_date,
    )
    entities = []
    if occupancy:
        entities.extend([position, occupancy])
    return entities


def make_person(
    context: Context,
    first_name: str,
    last_name: str,
    primary: Optional[str],
    secondary: Optional[str],
) -> Entity:
    positions = sorted(p for p in [primary, secondary] if p is not None)
    position = positions[0] if positions else None
    person = context.make("Person")
    person.id = context.make_id(first_name, last_name, position)
    h.apply_name(person, first_name=first_name, last_name=last_name)
    return person


def dict_keys_by_prefix(data: dict, prefix: str) -> dict[str:Any]:
    """
    Returns a new dict with keys and values from the original matching the prefix.
    The prefix is removed from the keys in the new dict.
    """
    return {
        k.removeprefix(prefix): data.pop(k)
        for k in list(data.keys())
        if k.startswith(prefix)
    }


def crawl_row(context, row, position_topics):
    position_entities = []

    primary_position_data = dict_keys_by_prefix(row, "Primary ")
    primary_position_name = make_position_name(primary_position_data)
    secondary_data = dict_keys_by_prefix(row, "Secondary ")
    secondary_position_name = make_position_name(secondary_data)

    person = make_person(
        context,
        row.pop("First Name"),
        row.pop("Last Name"),
        primary_position_name,
        secondary_position_name,
    )

    position_entities.extend(
        make_affiliation_entities(
            context,
            person,
            primary_position_name,
            primary_position_data,
            position_topics,
        )
    )
    position_entities.extend(
        make_affiliation_entities(
            context, person, secondary_position_name, secondary_data, position_topics
        )
    )

    context.audit_data(row)
    if position_entities:
        for entity in position_entities:
            context.emit(entity)
        context.emit(person, target=True)


def crawl_file(
    context: Context,
    url,
    filename,
    fields,
    expected_headings,
    position_topics,
):
    path = context.fetch_resource(filename, url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)

    with open(path, encoding="utf-8-sig") as fh:
        headings = next(csv.reader(fh, delimiter=";"))
        assert headings == expected_headings, (url, headings)
        reader = csv.DictReader(fh, fieldnames=fields, delimiter=";")
        for row in reader:
            crawl_row(context, row, position_topics)


def crawl(context: Context):
    # Register of appointed civil servants
    crawl_file(
        context,
        url="https://www.sukobinteresa.hr/export/registar_rukovodecih_drzavnih_sluzbenika_koje_imenuje_vlada_republike_hrvatske.csv",
        filename="appointed.csv",
        fields=FIELDS_CIVIL_SERVANTS,
        expected_headings=EXPECTED_COLUMNS_CIVIL_SERVANTS,
        position_topics=["gov.admin"],
    )
    # Register of obligors
    crawl_file(
        context,
        url="https://www.sukobinteresa.hr/export/registar_duznosnika.csv",
        filename="obligated.csv",
        fields=FIELDS_OBLIGATED_PERSONS,
        expected_headings=EXPECTED_COLUMNS_OBLIGATED,
        position_topics=[],
    )
