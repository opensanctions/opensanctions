import csv
from typing import Any, Optional

from pantomime.types import CSV

from zavod import Context, Entity
from zavod import helpers as h
from zavod.logic.pep import categorise

DEDUPED_COLUMN_NAMES = [
    "Ime",
    "Prezime",
    "Primarna dužnost",  # first affiliation
    "Primarna Pravna osoba u kojoj obnaša dužnost",
    "Primarna Datum početka obnašanja dužnosti",
    "Primarna Datum kraja obnašanja dužnosti",
    "Sekundarna dužnost",  # second affiliation
    "Sekundarna Pravna osoba u kojoj obnaša dužnost",
    "Sekundarna Datum početka obnašanja dužnosti",
    "Sekundarna Datum kraja obnašanja dužnosti",
]

EXPECTED_COLUMNS = [
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

DATE_FORMATS = ["%d/%m/%Y"]


def make_legal_entity(context: Context, legal_entity_name: str) -> Optional[Entity]:
    legal_entity: Optional[Entity] = None
    if legal_entity_name:
        legal_entity = context.make("LegalEntity")
        legal_entity.id = context.make_slug(legal_entity_name)
        legal_entity.add("name", legal_entity_name)
        context.emit(legal_entity)
    return legal_entity


def make_affiliation_entities(
    context: Context, person: Entity, data: dict
) -> tuple[Optional[Entity]]:
    """Creates Position and Occupancy provided that the Occupancy meets OpenSanctions criteria.
    * A position's name include the title and optionally the name of the legal entity
    * A position with a legal entity but no title is titled 'Unknown position'
    * All positions (and Occupancies, Persons) are assumed to be Croatian
    * Positions with start and/or end date but no position name or legal entity name are discarded
    """

    title = data.pop("dužnost")
    legal_entity_name = data.pop("Pravna osoba u kojoj obnaša dužnost")
    start_date = data.pop("Datum početka obnašanja dužnosti")
    end_date = data.pop("Datum kraja obnašanja dužnosti")
    context.audit_data(data)

    if not any([title, legal_entity_name]):
        return None, None

    position_name = title or "Unknown position"
    if legal_entity_name:
        position_name = f"{position_name}, {legal_entity_name}"

    position = h.make_position(context, position_name, topics=None, country="HR")

    categorisation = categorise(context, position, is_pep=True)
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        no_end_implies_current=True,
        categorisation=categorisation,
        propagate_country=True,
        start_date=h.parse_date(start_date, DATE_FORMATS).pop(),
        end_date=h.parse_date(end_date, DATE_FORMATS).pop(),
    )
    if occupancy:
        return position, occupancy
    return None, None


def make_person(context: Context, first_name: str, last_name: str) -> Entity:
    person = context.make("Person")
    person.id = context.make_slug(first_name, last_name)
    person.add("firstName", first_name)
    person.add("lastName", last_name)
    return person


def validate_column_names(context: Context, column_names: list) -> None:
    """Raises AssertionError if expected columns are missing or not in the expected order.
    If the columns are present, but extra columns are also present, allows the crawl to continue
    but logs the extra columns.
    Note: This CSV has duplicate column names.
    """
    error_message = f"Column names do not match: {column_names}"
    assert column_names[: len(EXPECTED_COLUMNS)] == EXPECTED_COLUMNS, error_message
    if len(column_names) > len(EXPECTED_COLUMNS):
        context.log.warn(
            f"Unexpected column headers: {column_names[:len(EXPECTED_COLUMNS):]}"
        )


def extract_dict_keys_by_prefix(
    data: dict, key_names: list[str], prefix: str
) -> dict[str:Any]:
    """Pops dict keys in `key_names` if the key starts with `prefix`.
    Returns a new dict and removes keys from old dict"""
    return {
        k.removeprefix(prefix): data.pop(k) for k in key_names if k.startswith(prefix)
    }


def crawl(context: Context):
    """Fetches the current CSV file and crawls each row, making persons, occupancies and positions"""
    file_path = context.fetch_resource("daily_csv_release", context.data_url)
    context.export_resource(file_path, CSV, title=context.SOURCE_TITLE)
    with open(file_path, encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh, fieldnames=DEDUPED_COLUMN_NAMES, delimiter=";")
        column_names = list(next(reader).values())
        validate_column_names(context, column_names)
        for row in reader:
            person = make_person(context, row.pop("Ime"), row.pop("Prezime"))

            affiliation_1 = extract_dict_keys_by_prefix(
                row, DEDUPED_COLUMN_NAMES, "Primarna "
            )
            position_1, occupancy_1 = make_affiliation_entities(
                context, person, affiliation_1
            )

            affiliation_2 = extract_dict_keys_by_prefix(
                row, DEDUPED_COLUMN_NAMES, "Sekundarna "
            )
            position_2, occupancy_2 = make_affiliation_entities(
                context, person, affiliation_2
            )

            context.audit_data(row)

            if occupancy_1 or occupancy_2:
                [
                    context.emit(ent)
                    for ent in [position_1, position_2, occupancy_1, occupancy_2]
                    if ent is not None
                ]
                context.emit(person, target=True)
