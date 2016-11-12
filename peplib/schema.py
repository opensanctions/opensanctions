from jsonschema import Draft4Validator, FormatChecker, RefResolver

from peplib.config import SCHEMA_FIXTURES

format_checker = FormatChecker()
resolver = RefResolver('file://%s/' % SCHEMA_FIXTURES, {})


@format_checker.checks('country-code')
def is_country_code(code):
    if code is None:
        return True
    if not len(code.strip()):
        return False
    # TODO
    return True


def validate(data):
    _, schema = resolver.resolve('entity.json')
    validator = Draft4Validator(schema, resolver=resolver,
                                format_checker=format_checker)
    return validator.validate(data, schema)
