import json
from copy import deepcopy


class EntityManager(object):

    def __init__(self, engine):
        self.engine = engine
        self._entities = engine['pep_entity']
        self._other_names = engine['pep_other_name']
        self._identities = engine['pep_identity']
        self._addresses = engine['pep_address']

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

    def clear_entities(self, entities):
        uids = [e.get('uid') for e in entities]
        self._clear_table(self._entities, uids)
        self._clear_table(self._identities, uids)
        self._clear_table(self._other_names, uids)
        self._clear_table(self._addresses, uids)

    def save_entities(self, entities):
        self.clear_entities(entities)

        identities = []
        other_names = []
        addresses = []

        for entity in entities:
            entity['json'] = json.dumps(entity)
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

        self._entities.insert_many(self._pad_rows(entities))
        self._identities.insert_many(self._pad_rows(identities))
        self._other_names.insert_many(self._pad_rows(other_names))
        self._addresses.insert_many(self._pad_rows(addresses))
