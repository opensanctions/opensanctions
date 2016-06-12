import sys
import unicodecsv
import logging
import xlrd
from datetime import datetime

from peplib import Source
from peplib.util import make_id
from peplib.text import combine_name
from peplib.country import normalize_country

log = logging.getLogger(__name__)
source = Source('gb-hmt-sanc')

SOURCE = {
    'publisher': 'HM Treasury',
    'publisher_url': 'https://www.gov.uk/government/publications/financial-sanctions-consolidated-list-of-targets/consolidated-list-of-targets',
    'source': 'Consolidated Sanctions',
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


def parse_entry(group, rows):
    record = SOURCE.copy()
    record.update({
        'uid': make_id('gb', 'hmt', group),
        'identities': [],
        'addresses': [],
        'other_names': []
    })
    for row in rows:
        record.update({
            'type': row.pop('Group Type').lower(),
            'date_of_birth': parse_date(row.pop('DOB')),
            'place_of_birth': row.pop('Town of Birth'),
            'country_of_birth': normalize_country(row.pop('Country of Birth')),
            'nationality': normalize_country(row.get('Nationality')),
            'program': row.pop('Regime'),
            'summary': row.pop('Other Information'),
            'updated_at': parse_date(row.pop('Last Updated')),
            'function': row.pop('Position')
        })

        names = {
            'first_name': row.get('Name 1'),
            'second_name': row.get('Name 2'),
            'middle_name': row.get('Name 3'),
            'last_name': row.get('Name 6')
        }

        name = [row.pop('Title'), row.pop('Name 1'), row.pop('Name 2'),
                row.pop('Name 3'), row.pop('Name 4'), row.pop('Name 5'),
                row.pop('Name 6')]
        name = combine_name(*name)

        if 'name' not in record:
            record['name'] = name
            record.update(names)
        else:
            names['other_name'] = name
            names['type'] = row.pop('Alias Type')
            record['other_names'].append(names)

        addr = [row.pop('Address 1'), row.pop('Address 2'),
                row.pop('Address 3'), row.pop('Address 4'),
                row.pop('Address 5'), row.pop('Address 6')]
        addr = combine_name(*addr)
        if len(addr):
            record['addresses'].append({
                'text': addr,
                'postal_code': row.pop('Post/Zip Code')
            })

        if row.get('Passport Details'):
            record['identities'].append({
                'type': 'Passport',
                'number': row.pop('Passport Details'),
                'country': normalize_country(row.get('Nationality'))
            })

        if row.get('NI Number'):
            record['identities'].append({
                'type': 'NI',
                'number': row.pop('NI Number'),
                'country': normalize_country(row.get('Country'))
            })

        # from pprint import pprint
        # pprint(row)
    source.emit(record)


def hmt_parse(csvfile):
    groups = {}
    with open(csvfile, 'r') as fh:
        fh.readline()
        for row in unicodecsv.DictReader(fh):
            group = int(float(row.pop('Group ID')))
            if group not in groups:
                groups[group] = []
            groups[group].append(row)

    for group, rows in groups.items():
        parse_entry(group, rows)


if __name__ == '__main__':
    hmt_parse(sys.argv[1])
