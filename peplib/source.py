import os
import json
import logging
import countrynames
from pprint import pprint  # noqa

from peplib.util import clean_obj, unique_objs
from peplib.config import JSON_PATH
from peplib.schema import validate


log = logging.getLogger(__name__)


class Source(object):

    def __init__(self, source_id):
        self.source_id = source_id
        self.out_path = os.path.join(JSON_PATH, source_id)
        try:
            os.makedirs(self.out_path)
        except:
            pass
        self.entity_count = 0
        self.log = logging.getLogger(__name__)

    def clear(self):
        pass

    def emit(self, data):
        data['identities'] = unique_objs(data.pop('identities', []))
        data['other_names'] = unique_objs(data.get('other_names', []))
        data['addresses'] = unique_objs(data.get('addresses', []))
        data = clean_obj(data)
        validate(data)
        pprint(data)
        # entity_file = os.path.join(self.out_path, '%s.json' % data.get('uid'))
        # with open(entity_file, 'w') as fh:
        #     json.dump(data, fh, indent=2)
        self.entity_count += 1

    def normalize_country(self, name):
        return countrynames.to_code(name)

    def save(self):
        self.log.info("Parsed %s entities", self.entity_count)
