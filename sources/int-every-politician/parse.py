import json
import logging
from datetime import datetime

from pepparser.util import make_id
from pepparser.country import normalize_country

log = logging.getLogger(__name__)

PUBLISHER = {
    'publisher': 'mySociety',
    'publisher_url': 'https://www.mysociety.org/',
    'source': 'EveryPolitician.org',
    'source_id': 'EVERY-POLITICIAN'
}


def parse_ts(ts):
    return datetime.fromtimestamp(int(ts)).date().isoformat()


def everypolitician_parse(emit, json_file):
    with open(json_file, 'r') as fh:
        data = json.load(fh)

    for policitian in data.get('politicians'):
        # from pprint import pprint
        # pprint(policitian)

        # TODO: add politician
        country = normalize_country(policitian.get('country_code'))
        entity = {
            'uid': make_id('evpo', policitian.get('id').split('-')[-1]),
            'name': policitian.get('name'),
            'type': 'individual',
            'addresses': [{'country': country}],
            'updated_at': parse_ts(policitian.get('legislature_lastmod')),
            'source_url': policitian.get('source_url')
        }
        entity.update(PUBLISHER)
        emit.entity(entity)
