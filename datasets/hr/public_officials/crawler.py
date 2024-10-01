import csv
from typing import Any, List, Optional

from rigour.mime.types import CSV

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


def make_position_name(data: dict) -> Optional[str]:
    title = data.pop("dužnost")
    legal_entity_name = data.pop("Pravna osoba u kojoj obnaša dužnost")
    if not any([title, legal_entity_name]):
        return None

    position_name = title or "Unknown position"
    if legal_entity_name:
        position_name = f"{position_name}, {legal_entity_name}"
    return position_name


def make_affiliation_entities(
    context: Context, person: Entity, position_name: str, data: dict
) -> List[Entity]:
    """Creates Position and Occupancy provided that the Occupancy meets OpenSanctions criteria.
    * A position's name include the title and optionally the name of the legal entity
    * A position with a legal entity but no title is titled 'Unknown position'
    * All positions (and Occupancies, Persons) are assumed to be Croatian
    * Positions with start and/or end date but no position name or legal entity name are discarded
    """

    start_date = data.pop("Datum početka obnašanja dužnosti")
    end_date = data.pop("Datum kraja obnašanja dužnosti")
    context.audit_data(data)

    position = h.make_position(context, position_name, topics=None, country="HR")

    categorisation = categorise(context, position, is_pep=True)
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        no_end_implies_current=True,
        categorisation=categorisation,
        propagate_country=True,
        start_date=h.extract_date(context.dataset, start_date).pop(),
        end_date=h.extract_date(context.dataset, end_date).pop(),
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
            position_entities = []

            primary_data = extract_dict_keys_by_prefix(
                row, DEDUPED_COLUMN_NAMES, "Primarna "
            )
            primary_position_name = make_position_name(primary_data)
            secondary_data = extract_dict_keys_by_prefix(
                row, DEDUPED_COLUMN_NAMES, "Sekundarna "
            )
            secondary_position_name = make_position_name(secondary_data)
            person = make_person(
                context,
                row.pop("Ime"),
                row.pop("Prezime"),
                primary_position_name,
                secondary_position_name,
            )

            position_entities.extend(
                make_affiliation_entities(
                    context, person, primary_position_name, primary_data
                )
            )
            position_entities.extend(
                make_affiliation_entities(
                    context, person, secondary_position_name, secondary_data
                )
            )

            context.audit_data(row)

            if position_entities:
                for entity in position_entities:
                    context.emit(entity)
                context.emit(person, target=True)
