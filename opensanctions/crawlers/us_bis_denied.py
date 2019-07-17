import csv
from pprint import pprint  # noqa

from opensanctions.util import EntityEmitter
from opensanctions.util import jointext, normalize_country


def parse_row(emitter, row):
    entity = emitter.make('LegalEntity')
    entity.make_id(row.get('Effective_Date'), row.get('Name'))
    entity.add('name', row.get('Name'))
    entity.add('notes', row.get('Action'))
    entity.add('country', normalize_country(row.get('Country')))
    # entity.updated_at = row.get('Effective_Date')

    address = jointext(row.get('Street_Address'),
                       row.get('Postal_Code'),
                       row.get('City'),
                       row.get('State'),
                       sep=', ')
    entity.add('address', address)
    emitter.emit(entity)

    sanction = emitter.make('Sanction')
    sanction.make_id(entity.id, row.get('FR_Citation'))
    sanction.add('entity', entity)
    sanction.add('program', row.get('FR_Citation'))
    sanction.add('authority', 'US Bureau of Industry and Security')
    sanction.add('country', 'us')
    sanction.add('startDate', row.get('Effective_Date'))
    emitter.emit(sanction)


def parse(context, data):
    emitter = EntityEmitter(context)
    with context.http.rehash(data) as res:
        with open(res.file_path, 'r') as csvfile:
            for row in csv.DictReader(csvfile, delimiter='\t'):
                parse_row(emitter, row)
    emitter.finalize()
