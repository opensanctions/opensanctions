from jsonschema import Draft4Validator, FormatChecker, RefResolver

from peplib.config import SCHEMA_FIXTURES
from peplib.country import load_countries

format_checker = FormatChecker()
resolver = RefResolver('file://%s/' % SCHEMA_FIXTURES, {})


@format_checker.checks('country-code')
def is_country_code(code):
    if code is None or not len(code.strip()):
        return False
    return code.upper() in set(load_countries().values())


def validate(data):
    _, schema = resolver.resolve('entity.json')
    validator = Draft4Validator(schema, resolver=resolver,
                                format_checker=format_checker)
    return validator.validate(data, schema)
