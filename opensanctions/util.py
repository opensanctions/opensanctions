import logging
import countrynames
from followthemoney import model

log = logging.getLogger(__name__)


class EntityEmitter(object):

    def __init__(self, context):
        self.context = context

    def make(self, schema):
        key_prefix = self.context.crawler.name
        entity = model.make_entity(schema, key_prefix=key_prefix)
        return entity

    def emit(self, entity, rule='pass'):
        data = entity.to_dict()
        if entity.id is None:
            log.warning("Entity has no ID: %r", data)
        self.context.emit(rule=rule, data=data)


class Constants(object):
    TYPE_PASSPORT = u'passport'
    TYPE_NATIONALID = u'nationalid'
    TYPE_OTHER = u'other'

    GENDER_MALE = 'male'
    GENDER_FEMALE = 'female'


def normalize_country(name):
    return countrynames.to_code(name)
