import csv
from pprint import pprint  # noqa

from opensanctions.util import EntityEmitter, normalize_country


def parse_row(emitter, row):
    entity = emitter.make('LegalEntity')
    entity.make_id(row.get('Effective_Date'), row.get('Name'))
    entity.add('name', row.get('Name'))
    entity.add('summary', row.get('Action'))
    entity.add('program', row.get('FR_Citation'))
    entity.add('country', normalize_country(row.get('Country')))
    # entity.updated_at = row.get('Effective_Date')

    address = (row.get('Street_Address'),
               row.get('Postal_Code'),
               row.get('City'),
               row.get('State'))
    address = ', '.join(address)
    entity.add('address', address)
    emitter.emit(entity)


def parse(context, data):
    emitter = EntityEmitter(context)
    with context.http.rehash(data) as res:
        with open(res.file_path, 'r') as csvfile:
            for row in csv.DictReader(csvfile, delimiter='\t'):
                parse_row(emitter, row)
