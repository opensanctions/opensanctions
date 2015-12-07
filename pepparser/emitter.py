import os
import json
import logging
from pprint import pprint  # noqa

from jsonschema import Draft4Validator, FormatChecker, RefResolver

from pepparser.util import clean_obj, unique_objs, SCHEMA_FIXTURES
from pepparser.country import load_countries


log = logging.getLogger(__name__)

BASE_URI = 'http://schema.opennames.org/'
ENTITY_SCHEMA = 'http://schema.opennames.org/entity.json#'

format_checker = FormatChecker()
resolver = RefResolver(ENTITY_SCHEMA, {})

for file_name in os.listdir(SCHEMA_FIXTURES):
    with open(os.path.join(SCHEMA_FIXTURES, file_name), 'r') as fh:
        schema = json.load(fh)
        resolver.store[schema.get('id')] = schema


@format_checker.checks('country-code')
def is_country_code(code):
    if code is None or not len(code.strip()):
        return False
    return code.upper() in set(load_countries().values())


def validate(resolver, data, schema):
    _, schema = resolver.resolve(schema)
    validator = Draft4Validator(schema, resolver=resolver,
                                format_checker=format_checker)
    return validator.validate(data, schema)


class Emitter(object):

    def __init__(self, engine):
        self.engine = engine
        self.table_entities = engine['pep_entity']
        self.table_other_names = engine['pep_other_name']
        self.table_indentities = engine['pep_identity']
        self.table_addresses = engine['pep_address']
        self.entities = []

    def entity(self, data):
        data['identities'] = unique_objs(data.get('identities'))
        data['other_names'] = unique_objs(data.get('other_names'))
        data['addresses'] = unique_objs(data.get('addresses'))
        data = clean_obj(data)
        validate(resolver, data, ENTITY_SCHEMA)
        # pprint(data)
        self.entities.append(data)

    def _clear_table(self, table, uids):
        if 'uid' not in table.table.c:
            return
        q = table.table.delete().where(table.table.c.uid.in_(uids))
        self.engine.executable.execute(q)

    def _pad_rows(self, rows):
        keys = set([])
        for row in rows:
            keys.update(row.keys())
        for row in rows:
            for key in keys:
                if key not in row:
                    row[key] = None
        return rows

    def close(self):
        log.info("Parsed %s entities", len(self.entities))
        uids = [e.get('uid') for e in self.entities]

        identities = []
        other_names = []
        addresses = []

        for entity in self.entities:
            base = {
                'uid': entity.get('uid'),
                'name': entity.get('name')
            }

            for identity in entity.pop('identities', []):
                identity.update(base)
                identities.append(identity)

            for other_name in entity.pop('other_names', []):
                other_name.update(base)
                other_names.append(other_name)

            for address in entity.pop('addresses', []):
                address.update(base)
                addresses.append(address)

        self._clear_table(self.table_entities, uids)
        self._clear_table(self.table_indentities, uids)
        self._clear_table(self.table_other_names, uids)
        self._clear_table(self.table_addresses, uids)

        self.table_entities.insert_many(self._pad_rows(self.entities))
        self.table_indentities.insert_many(self._pad_rows(identities))
        self.table_other_names.insert_many(self._pad_rows(other_names))
        self.table_addresses.insert_many(self._pad_rows(addresses))
