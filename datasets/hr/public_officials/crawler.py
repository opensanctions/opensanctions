import csv
from typing import Any, List, Optional

from rigour.mime.types import CSV

from zavod import Context, Entity
from zavod import helpers as h
from zavod.logic.pep import categorise


CIVIL_SERVANTS_URL = "https://www.sukobinteresa.hr/export/registar_rukovodecih_drzavnih_sluzbenika_koje_imenuje_vlada_republike_hrvatske.csv"
DEDUPED_COLUMN_NAMES_PUB = [
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

DEDUPED_COLUMN_NAMES_CIV = [
    "Ime",
    "Prezime",
    "Dužnost",
    "Pravna osoba u kojoj obnaša dužnost",
    "Datum početka obnašanja dužnosti",
    "Datum kraja obnašanja dužnosti",
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
    title = data.pop("dužnost", None)
    if title is None:
        title = data.pop("Dužnost")
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
    # Register of appointed civil servants
    file_path = context.fetch_resource("source.csv", CIVIL_SERVANTS_URL)
    context.export_resource(file_path, CSV, title=context.SOURCE_TITLE)
    with open(file_path, encoding="utf-8") as fh:
        reader = csv.DictReader(fh, fieldnames=DEDUPED_COLUMN_NAMES_CIV, delimiter=";")
        # Skip the first row
        next(reader, None)
        for row in reader:
            # Filter out fields where the key is None
            filtered_row = {k: v for k, v in row.items() if k is not None}
            position_entities = []
            position_name = make_position_name(filtered_row)

            person = make_person(
                context,
                filtered_row.pop("Ime"),
                filtered_row.pop("Prezime"),
                position_name,
                secondary=None,
            )
            person.add("topics", "gov.admin")
            position_entities.extend(
                make_affiliation_entities(context, person, position_name, filtered_row)
            )
            context.audit_data(filtered_row)

            if position_entities:
                for entity in position_entities:
                    context.emit(entity)
                context.emit(person, target=True)

    # Register of obligors
    file_path = context.fetch_resource("daily_csv_release", context.data_url)
    context.export_resource(file_path, CSV, title=context.SOURCE_TITLE)
    with open(file_path, encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh, fieldnames=DEDUPED_COLUMN_NAMES_PUB, delimiter=";")
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
