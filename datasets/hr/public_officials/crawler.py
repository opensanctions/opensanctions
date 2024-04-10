import csv
from datetime import datetime
from typing import Any, Optional, Mapping, Sequence

from pantomime.types import CSV
from zavod import Context, Entity
from zavod import helpers as h
#from zavod.logic.pep import categorise

FIELD_NAMES = [
                'Ime', 
                'Prezime', 
                'Primarna dužnost', # first affiliation
                'Primarna Pravna osoba u kojoj obnaša dužnost',
                'Primarna Datum početka obnašanja dužnosti',
                'Primarna Datum kraja obnašanja dužnosti',
                'Sekundarna dužnost', # second affiliation
                'Sekundarna Pravna osoba u kojoj obnaša dužnost',
                'Sekundarna Datum početka obnašanja dužnosti',
                'Sekundarna Datum kraja obnašanja dužnosti',
]

def convert_date(date:Optional[str]) -> Optional[str]:
        return datetime.strptime(date, '%d/%m/%Y') if date else None

def make_office(context:Context, data:dict) -> Entity:
    """_summary_

    Args:
        context (Context) zavod crawler context
        data (dict) with keys `dužnost`, `Pravna osoba u kojoj obnaša dužnost`,
        `Datum početka obnašanja dužnosti` and `Datum kraja obnašanja dužnosti'`

    Returns:
        Entity: position entity
    """
    legal_entity:Optional[Entity] = None
    if legal_entity_name := data.pop("Pravna osoba u kojoj obnaša dužnost"):
        legal_entity = context.make("LegalEntity")
        legal_entity.id = context.make_slug(legal_entity_name)
        legal_entity.add('name', legal_entity_name)
        context.emit(legal_entity)

    position:Optional[Entity] = None
    position_name = data.pop('dužnost') or 'Unspecified position'
    position_start = convert_date(data.pop("Datum početka obnašanja dužnosti"))
    position_end = convert_date(data.pop("Datum kraja obnašanja dužnosti"))
    position = h.make_position(context,
                            position_name, 
                            topics=None,
                            organization=legal_entity,
                            country='HR',
                            inception_date=position_start,
                            dissolution_date=position_end
                            )
    context.emit(position)
    context.audit_data(data)
    return position

def make_person(context:Context, first_name:str, last_name:str) -> Entity:
    person = context.make("Person")
    person.id = context.make_slug(first_name, last_name)
    person.add("firstName", first_name)
    person.add("lastName", last_name)
    person.add("country", 'LT')
    person.add("topics", "role.pep")
    return person

def validate_headers(context:Context, headers:list) -> None:
    """Raises AssertionError if columns are missing or not in the expected order.
    Logs extra columns. Note: This CSV has duplicate column names. 
    """
    error_message = f"Unexpected column headers: {headers}"
    assert headers[:len(FIELD_NAMES)] == FIELD_NAMES, error_message
    if len(headers) > len(FIELD_NAMES):
        context.log.warn(f"Unexpected column headers: {headers[:len(FIELD_NAMES):]}")

def crawl(context: Context):
    """Fetches the current CSV file and crawls each row, making persons, legal entities, and positions"""
    file_path = context.fetch_resource("daily_csv_release", context.data_url)
    context.export_resource(file_path, CSV, title=context.SOURCE_TITLE)
    with open(file_path, 'r',  encoding='utf-8-sig') as fh:
        reader = csv.DictReader(fh, fieldnames=FIELD_NAMES, delimiter=';')
        column_headers = next(reader)
        for row in reader:
            person = make_person(context, row.pop('Ime'), row.pop('Prezime'))

            affiliation_1 = {k.removeprefix('Primarna '):row.pop(k) for k in FIELD_NAMES if k.startswith('Primarna')}
            if position_1 := make_office(context, affiliation_1):
                person.add('position', position_1)

            affiliation_2 = {k.removeprefix('Sekundarna '):row.pop(k) for k in FIELD_NAMES if k.startswith('Sekundarna')}
            if position_2 := make_office(context, affiliation_2):
                person.add('position', position_2)

            # TODO add occupancies
            context.audit_data(row)
            context.emit(person, target=True)