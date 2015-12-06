import os
import json
from pprint import pprint  # noqa

from jsonschema import Draft4Validator, FormatChecker, RefResolver

from pepparser.util import clean_obj, SCHEMA_FIXTURES
from pepparser.country import load_countries

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

    def __init__(self):
        pass

    def entity(self, data):
        data = clean_obj(data)
        validate(resolver, data, ENTITY_SCHEMA)
        # pprint(data)
