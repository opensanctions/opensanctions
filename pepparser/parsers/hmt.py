import unicodecsv
import logging
import xlrd
from dateutil.parser import parse as dateutil_parse
from datetime import datetime

from pepparser.util import remove_namespace, make_id
from pepparser.text import combine_name
from pepparser.country import normalize_country

log = logging.getLogger(__name__)


SOURCE = {
    'publisher': 'HM Treasury',
    'publisher_url': 'https://www.gov.uk/government/publications/financial-sanctions-consolidated-list-of-targets/consolidated-list-of-targets',
    'source': 'Consolidated Sanctions list',
    'source_url': 'http://hmt-sanctions.s3.amazonaws.com/sanctionsconlist.htm'
}


def parse_date(date):
    if date is None or not len(date.strip()):
        return
    if date.startswith('00/'):
        return date[6:]
    try:
        num = int(float(date))
        year, month, day, hour, minute, second = \
            xlrd.xldate_as_tuple(num, 0)
        dt = datetime(year, month, day, hour, minute, second)
        return dt.date().isoformat()
    except Exception as xle:
        log.exception(xle)


def parse_entry(emit, record, row):
    # from pprint import pprint
    # pprint(row)
    record.update({
        'uid': make_id('gb', 'hmt', int(float(row.get('Group ID')))),
        'type': row.get('Group Type').lower(),
        'date_of_birth': parse_date(row.get('DOB')),
        'place_of_birth': row.get('Town of Birth'),
        'country_of_birth': normalize_country(row.get('Country of Birth')),
        'program': row.get('Regime'),
        'summary': row.get('Other Information'),
        'updated_at': parse_date(row.get('Last Update')),
        'function': row.get('Position'),
        'first_name': row.get('Name 1'),
        'second_name': row.get('Name 2'),
        'middle_name': row.get('Name 3'),
        'last_name': row.get('Name 6'),
        'identities': []
    })

    name = [row.get('Title'), row.get('Name 1'), row.get('Name 2'),
            row.get('Name 3'), row.get('Name 4'), row.get('Name 5'),
            row.get('Name 6')]
    record['name'] = combine_name(*name)

    if row.get('Passport Details'):
        record['identities'].append({
            'type': 'Passport',
            'number': row.get('Passport Details'),
            'country': normalize_country(row.get('Nationality'))
        })

    if row.get('NI'):
        record['identities'].append({
            'type': 'NI',
            'number': row.get('Passport Details'),
            'country': normalize_country(row.get('Country'))
        })

    emit.entity(record)


def hmt_parse(emit, csvfile):
    with open(csvfile, 'r') as fh:
        fh.readline()
        for row in unicodecsv.DictReader(fh):
            record = SOURCE.copy()
            parse_entry(emit, record, row)
