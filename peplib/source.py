import logging
import dataset
import countrynames
from pprint import pprint  # noqa

from peplib.util import clean_obj, unique_objs
from peplib.config import DATABASE_URI
from peplib.schema import validate


log = logging.getLogger(__name__)
db = dataset.connect(DATABASE_URI)


class Source(object):

    def __init__(self, source_id):
        self.source_id = source_id
        self.log = logging.getLogger(source_id)
        self.entity_count = 0
        self.entity_table = db[source_id]
        self.identities_table = db[source_id + '_identities']
        self.other_names_table = db[source_id + '_other_names']
        self.addresses_table = db[source_id + '_addresses']

    def clear(self):
        self.entity_table.delete()
        self.identities_table.delete()
        self.other_names_table.delete()
        self.addresses_table.delete()

    def emit(self, data):
        data['identities'] = unique_objs(data.get('identities', []))
        data['other_names'] = unique_objs(data.get('other_names', []))
        data['addresses'] = unique_objs(data.get('addresses', []))
        data = clean_obj(data)
        validate(data)
        uid = data.get('uid')

        for identity in data.pop('identities', []):
            identity['uid'] = uid
            self.identities_table.insert(identity)

        for other_name in data.pop('other_names', []):
            other_name['uid'] = uid
            self.other_names_table.insert(other_name)

        for address in data.pop('addresses', []):
            address['uid'] = uid
            self.addresses_table.insert(address)

        self.entity_table.insert(data)
        self.entity_count += 1

    def normalize_country(self, name):
        return countrynames.to_code(name)

    def save(self):
        self.log.info("Parsed %s entities", self.entity_count)
