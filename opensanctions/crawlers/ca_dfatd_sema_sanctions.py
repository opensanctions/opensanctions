from pprint import pprint  # noqa
from normality import collapse_spaces

from opensanctions.util import EntityEmitter
from opensanctions.util import normalize_country, jointext


def parse(context, data):
    emitter = EntityEmitter(context)
    with context.http.rehash(data) as result:
        for node in result.xml.findall('.//record'):
            parse_entry(emitter, node)
    emitter.finalize()


def parse_entry(emitter, node):
    # ids are per country and entry type (individual/entity)
    country = node.findtext('./Country')
    if ' / ' in country:
        country, _ = country.split(' / ')
    country_code = normalize_country(country)
    entity_name = node.findtext('./Entity')
    item = node.findtext('.//Item')

    entity = emitter.make('LegalEntity')
    if entity_name is None:
        entity = emitter.make('Person')
    entity.make_id(country, entity_name, item)
    entity.add('name', entity_name)
    entity.add('country', country_code)

    sanction = emitter.make('Sanction')
    sanction.make_id(entity.id)
    sanction.add('entity', entity)
    sanction.add('authority', 'Canadian international sanctions')
    sanction.add('program', node.findtext('.//Schedule'))

    given_name = node.findtext('.//GivenName')
    entity.add('firstName', given_name, quiet=True)
    last_name = node.findtext('.//LastName')
    entity.add('lastName', last_name, quiet=True)
    entity.add('name', jointext(given_name, last_name))

    dob = node.findtext('.//DateOfBirth')
    if dob is not None:
        dob = '-'.join(reversed(dob.split('/')))
        entity.add('birthDate', dob, quiet=True)

    names = node.findtext('.//Aliases')
    if names is None:
        return

    for name in names.split(', '):
        name = collapse_spaces(name)
        entity.add('alias', name)

    emitter.emit(entity)
    emitter.emit(sanction)
