import csv
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any, Optional

from pantomime.types import CSV

from zavod import Context, Entity
from zavod import helpers as h
from zavod.logic.pep import categorise

FIELD_NAMES = [
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


def convert_date(date: Optional[str]) -> Optional[str]:
    return datetime.strptime(date, "%d/%m/%Y").strftime("%Y-%m-%d") if date else None


def make_legal_entity(context: Context, legal_entity_name: str) -> Optional[Entity]:
    legal_entity: Optional[Entity] = None
    if legal_entity_name:
        legal_entity = context.make("LegalEntity")
        legal_entity.id = context.make_slug(legal_entity_name)
        legal_entity.add("name", legal_entity_name)
        context.emit(legal_entity)
    return legal_entity


def handle_affiliation_data(context: Context, person: Entity, data: dict) -> None:
    """Creates, emits Position, Occupancy, and optionally a LegalEntity provided that the Occupancy meets
    OpenSanctions criteria.
    Note that the provided data is incomplete and the modeling reflects this:
      * A position name with no legal entity name results in a position with no Organization reference
      * A legal entity name with no position name results in an position named "Unspecified position"
         with organizational reference
      * A start and/or end date with no position name or legal entity name results in an unspecified position
         with no organizational reference. This is done for fidelity to the source.
    """
    legal_entity_name = data.pop("Pravna osoba u kojoj obnaša dužnost")
    position_name = data.pop("dužnost")
    start_date = data.pop("Datum početka obnašanja dužnosti")
    end_date = data.pop("Datum kraja obnašanja dužnosti")
    if any([legal_entity_name, position_name, start_date, end_date]):
        legal_entity = make_legal_entity(context, legal_entity_name)
        position_name = position_name or "Unspecified position"
        position = h.make_position(
            context,
            position_name,
            topics=None,
            organization=legal_entity,
        )

        categorisation = categorise(context, position, is_pep=True)
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            no_end_implies_current=True,
            categorisation=categorisation,
            propagate_country=True,
            start_date=convert_date(start_date),
            end_date=convert_date(end_date),
        )
        if occupancy:
            context.emit(position)
            context.emit(occupancy)
    context.audit_data(data)


def make_person(context: Context, first_name: str, last_name: str) -> Entity:
    person = context.make("Person")
    person.id = context.make_slug(first_name, last_name)
    person.add("firstName", first_name)
    person.add("lastName", last_name)
    person.add("country", "LT")
    person.add("topics", "role.pep")
    return person


def validate_headers(context: Context, headers: list) -> None:
    """Raises AssertionError if columns are missing or not in the expected order.
    Logs extra columns. Note: This CSV has duplicate column names.
    """
    error_message = f"Unexpected column headers: {headers}"
    assert headers[: len(FIELD_NAMES)] == FIELD_NAMES, error_message
    if len(headers) > len(FIELD_NAMES):
        context.log.warn(f"Unexpected column headers: {headers[:len(FIELD_NAMES):]}")


def extract_dict_keys_by_prefix(data: dict, key_names: list[str], prefix: str) -> dict[str:Any]:
    """Pops dict keys in `key_names` if the key starts with `prefix`.
    Returns a new dict and removes keys from old dict"""
    return {k.removeprefix(prefix): data.pop(k) for k in key_names if k.startswith(prefix)}


def crawl(context: Context):
    """Fetches the current CSV file and crawls each row, making persons, legal entities, and positions"""
    file_path = context.fetch_resource("daily_csv_release", context.data_url)
    context.export_resource(file_path, CSV, title=context.SOURCE_TITLE)
    with open(file_path, encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh, fieldnames=FIELD_NAMES, delimiter=";")
        column_headers = next(reader)
        for row in reader:
            person = make_person(context, row.pop("Ime"), row.pop("Prezime"))

            affiliation_1 = extract_dict_keys_by_prefix(row, FIELD_NAMES, "Primarna ")
            handle_affiliation_data(context, person, affiliation_1)

            affiliation_2 = extract_dict_keys_by_prefix(row, FIELD_NAMES, "Sekundarna ")
            handle_affiliation_data(context, person, affiliation_2)

            context.audit_data(row)
            context.emit(person, target=True)
