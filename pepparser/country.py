import os
from normality import slugify
import unicodecsv

from pepparser.util import DATA_FIXTURES

COUNTRIES = {}
REQUESTED = []


def load_countries():
    if len(COUNTRIES):
        return COUNTRIES
    with open(os.path.join(DATA_FIXTURES, 'countries.csv'), 'r') as fh:
        for row in unicodecsv.DictReader(fh):
            name = slugify(row['name'], sep=' ').strip()
            code = row['code'].strip().upper()
            REQUESTED.append({'name': row['name'], 'code': code})
            COUNTRIES[name] = code
    return COUNTRIES


def normalize_country(name):
    if name is None:
        return
    normed = slugify(name, sep=' ').strip()
    if not len(normed):
        return
    countries = load_countries()
    if normed in countries:
        return countries[normed]
    for req in REQUESTED:
        if req['name'] == name:
            return
    REQUESTED.append({'name': name, 'code': None})
    save_requested()


def save_requested():
    with open(os.path.join(DATA_FIXTURES, 'requested.csv'), 'w') as fh:
        fields = ['name', 'code']
        writer = unicodecsv.writer(fh)
        writer.writerow(fields)
        for row in sorted(REQUESTED, key=lambda d: d['name']):
            writer.writerow([row.get('name'), row.get('code')])
