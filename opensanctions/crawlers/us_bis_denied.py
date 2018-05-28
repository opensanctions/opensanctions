from pprint import pprint  # noqa

import csv
from memorious.helpers import make_id

from opensanctions.models import Entity


def parse_row(context, data):
    row = data.get('row')
    uid = make_id(row.get('Effective_Date'), row.get('Name'))
    entity = Entity.create('us-bis-denied', uid)
    entity.type = Entity.TYPE_ENTITY
    entity.name = row.get('Name')
    entity.updated_at = row.get('Effective_Date')
    entity.program = row.get('FR_Citation')
    entity.summary = row.get('Action')
    address = entity.create_address()
    address.street = row.get('Street_Address')
    address.postal_code = row.get('Postal_Code')
    address.region = row.get('State')
    address.city = row.get('City')
    address.country = row.get('Country')

    # pprint(entity.to_dict())
    context.emit(data=entity.to_dict())


def parse(context, data):
    res = context.http.rehash(data)
    with open(res.file_path, 'r') as csvfile:
        for row in csv.DictReader(csvfile, delimiter='\t'):
            context.emit(data={'row': row})
