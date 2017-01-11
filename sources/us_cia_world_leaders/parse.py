import sys
import json
import logging
from dateutil.parser import parse as dateutil_parse

from peplib import Source
from peplib.util import make_id

log = logging.getLogger(__name__)
source = Source('us_cia_world_leaders')

PUBLISHER = {
    'publisher': 'US CIA',
    'publisher_url': 'https://www.cia.gov/library/publications/resources/world-leaders-1/index.html',
    'source': 'World Leaders'
}


def parse_date(date):
    if date is None and len(date.strip()):
        return
    try:
        return dateutil_parse(date).date().isoformat()
    except:
        return


def worldleaders_parse(json_file):
    with open(json_file, 'r') as fh:
        data = json.load(fh)

    for leader in data.get('leaders'):
        if not len(leader.get('name')):
            log.warning('No name on entity: %r', leader)
            continue
        country = source.normalize_country(leader.get('country'))
        summary = leader.get('title')
        if leader.get('component'):
            summary = '%s (%s)' % (summary, leader.get('component'))
        entity = {
            'uid': make_id('us', 'cia', 'worldleaders', country,
                           leader.get('name')),
            'name': leader.get('name'),
            'nationality': country,
            'type': 'individual',
            'summary': summary,
            'updated_at': parse_date(leader.get('last_update')),
            'source_url': leader.get('url')
        }
        entity.update(PUBLISHER)
        source.emit(entity)


if __name__ == '__main__':
    worldleaders_parse(sys.argv[1])
