import logging
import balkhash
import countrynames
from normality import stringify
from followthemoney import model

log = logging.getLogger(__name__)


class EntityEmitter(object):

    def __init__(self, context):
        self.fragment = 0
        self.log = context.log
        self.name = context.crawler.name
        self.dataset = balkhash.init(self.name)
        self.bulk = self.dataset.bulk()

    def make(self, schema):
        entity = model.make_entity(schema, key_prefix=self.name)
        return entity

    def emit(self, entity, rule='pass'):
        if entity.id is None:
            raise RuntimeError("Entity has no ID: %r", entity)
        self.bulk.put(entity, fragment=str(self.fragment))
        self.fragment += 1

    def finalize(self):
        self.bulk.flush()


def normalize_country(name):
    return countrynames.to_code(name)


def jointext(*parts, sep=' '):
    parts = [stringify(p) for p in parts]
    parts = [p for p in parts if p is not None]
    return sep.join(parts)
