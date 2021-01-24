from ftmstore import get_dataset
from followthemoney import model

from opensanctions import settings


class Context(object):
    """A utility object to be passed into crawlers which supports
    emitting entities, accessing metadata and logging errors and
    warnings.
    """

    def __init__(self, dataset):
        self.dataset = dataset
        self.store = dataset.store
        self.bulk = self.store.bulk()
        self.fragment = 0

    def make(self, schema):
        return model.make_entity(schema, key_prefix=self.dataset.name)

    def emit(self, entity):
        if entity.id is None:
            raise RuntimeError("Entity has no ID: %r", entity)
        # pprint(entity.to_dict())
        fragment = str(self.fragment)
        self.bulk.put(entity, fragment=fragment)
        self.fragment += 1

    def close(self):
        self.bulk.flush()