import json
import logging
from datetime import datetime
from dateutil.parser import parse as dateutil_parse

from pepparser.util import make_id
from pepparser.text import combine_name
from pepparser.country import normalize_country

log = logging.getLogger(__name__)


SOURCE = {
    'publisher': 'Interpol',
    'publisher_url': 'http://www.interpol.int/notice/search/wanted',
    'source': 'Wanted Persons',
    'program': 'Red List',
    'type': 'individual'
}


def parse_case(emit, case):
    record = SOURCE.copy()
    url = case.get('url')
    name = combine_name(*reversed(case.get('name').split(', ')))
    updated = dateutil_parse(case.get('last_updated'))
    record.update({
        'uid': make_id('interpol', url.split('/')[-1]),
        'source_url': url,
        'name': name,
        'summary': case.get('reason'),
        'updated_at': updated.date().isoformat(),
        'place_of_birth': case.get('place_of_birth'),
        'gender': case.get('sex', '').lower(),
        'first_name': case.get('forename'),
        'last_name': case.get('present_family_name'),
        'nationality': normalize_country(case.get('nationality')),
        'identities': [],
        'addresses': [],
        'other_names': []
    })
    birth = case.get('date_of_birth').split(' ')[0]

    try:
        dt = datetime.strptime(birth, '%Y').date().isoformat()
        record['date_of_birth'] = dt
    except Exception:
        try:
            dt = datetime.strptime(birth, '%d/%m/%Y').date().isoformat()
            record['date_of_birth'] = dt
        except Exception as ex:
            log.exception(ex)

    emit.entity(record)


def interpol_parse(emit, jsonfile):
    with open(jsonfile, 'r') as fh:
        data = json.load(fh)

    for case in data['cases']:
        parse_case(emit, case)
