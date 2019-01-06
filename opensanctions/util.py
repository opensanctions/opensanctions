import logging
import countrynames
from normality import stringify
from followthemoney import model

log = logging.getLogger(__name__)


class EntityEmitter(object):

    def __init__(self, context):
        self.context = context
        self.log = context.log

    def make(self, schema):
        key_prefix = self.context.crawler.name
        entity = model.make_entity(schema, key_prefix=key_prefix)
        return entity

    def emit(self, entity, rule='pass'):
        data = entity.to_dict()
        if entity.id is None:
            log.warning("Entity has no ID: %r", data)
        self.context.emit(rule=rule, data=data)


def normalize_country(name):
    return countrynames.to_code(name)


def jointext(*parts, sep=' '):
    parts = [stringify(p) for p in parts]
    parts = [p for p in parts if p is not None]
    return sep.join(parts)
