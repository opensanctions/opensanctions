import sys
import unicodecsv
import logging

from peplib import Source
from peplib.util import make_id

log = logging.getLogger(__name__)
source = Source('us-bis-dpl')

SOURCE = {
    'publisher': 'US BIS',
    'publisher_url': 'https://www.bis.doc.gov/',
    'source': 'Denied Persons List',
    'source_url': 'https://www.bis.doc.gov/index.php/the-denied-persons-list',
    'type': 'entity'
}


def parse_row(row):
    record = SOURCE.copy()
    record.update({
        'uid': make_id('us', 'bis', row.get('Effective_Date'),
                       row.get('Name')),
        'name': row.get('Name'),
        'program': row.get('FR_Citation'),
        'summary': row.get('Action'),
        'updated_at': row.get('Last Update'),
        'nationality': source.normalize_country(row.get('Country')),
        'addresses': [{
            'address1': row.get('Street_Address'),
            'postal_code': row.get('Postal_Code'),
            'region': row.get('State'),
            'city': row.get('City'),
            'country': source.normalize_country(row.get('Country'))
        }]
    })
    source.emit(record)


def usbis_parse(csvfile):
    with open(csvfile, 'r') as fh:
        for row in unicodecsv.DictReader(fh, delimiter='\t'):
            parse_row(row)


if __name__ == '__main__':
    usbis_parse(sys.argv[1])
