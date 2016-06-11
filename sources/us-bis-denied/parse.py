import unicodecsv
import logging

from pepparser.util import make_id
from pepparser.country import normalize_country

log = logging.getLogger(__name__)


SOURCE = {
    'publisher': 'US BIS',
    'publisher_url': 'https://www.bis.doc.gov/',
    'source': 'Denied Persons List',
    'source_id': 'US-BIS-DPL',
    'source_url': 'https://www.bis.doc.gov/index.php/the-denied-persons-list',
    'type': 'entity'
}


def parse_row(emit, row):
    record = SOURCE.copy()
    record.update({
        'uid': make_id('us', 'bis', row.get('Effective_Date'),
                       row.get('Name')),
        'name': row.get('Name'),
        'program': row.get('FR_Citation'),
        'summary': row.get('Action'),
        'updated_at': row.get('Last Update'),
        'nationality': normalize_country(row.get('Country')),
        'addresses': [{
            'address1': row.get('Street_Address'),
            'postal_code': row.get('Postal_Code'),
            'region': row.get('State'),
            'city': row.get('City'),
            'country': normalize_country(row.get('Country'))
        }]
    })
    emit.entity(record)


def usbis_parse(emit, csvfile):
    with open(csvfile, 'r') as fh:
        for row in unicodecsv.DictReader(fh, delimiter='\t'):
            parse_row(emit, row)
