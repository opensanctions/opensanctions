import json
import logging
from dateutil.parser import parse as dateutil_parse

from pepparser.util import make_id
from pepparser.country import normalize_country

log = logging.getLogger(__name__)

PUBLISHER = {
    'publisher': 'US CIA',
    'publisher_url': 'https://www.cia.gov/library/publications/resources/world-leaders-1/index.html',
    'source': 'World Leaders',
    'source_id': 'US-CIA-LEADERS'
}


def parse_date(date):
    if date is None and len(date.strip()):
        return
    try:
        return dateutil_parse(date).date().isoformat()
    except:
        return


def worldleaders_parse(emit, json_file):
    with open(json_file, 'r') as fh:
        data = json.load(fh)

    for leader in data.get('leaders'):
        if not len(leader.get('name')):
            log.warning('No name on entity: %(title)s (%(country)s)', leader)
            continue
        country = normalize_country(leader.get('country'))
        summary = leader.get('title')
        if leader.get('component'):
            summary = '%s (%s)' % (summary, leader.get('component'))
        entity = {
            'uid': make_id('us', 'cia', 'worldleaders', country,
                           leader.get('name')),
            'name': leader.get('name'),
            'type': 'individual',
            'summary': summary,
            'addresses': [{'country': country}],
            'updated_at': parse_date(leader.get('last_update')),
            'source_url': leader.get('url')
        }
        entity.update(PUBLISHER)
        emit.entity(entity)
