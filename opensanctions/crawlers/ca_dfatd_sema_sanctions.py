from pprint import pprint  # noqa

from opensanctions.models import Entity, BirthDate
from normality import collapse_spaces


XML_URL = 'http://www.international.gc.ca/sanctions/assets/office_docs/sema-lmes.xml'  # noqa


def parse(context, data):
    res = context.http.rehash(data)
    doc = res.xml
    for node in doc.findall('.//record'):
        parse_entry(context, node)


def parse_entry(context, node):
    # ids are per country and entry type (individual/entity)
    country = get_country(node)
    id = str(node.findtext('.//Item'))
    if node.findtext('.//Entity') is None:
        type = 'ind'
    else:
        type = 'ent'
    entity = Entity.create('ca-dfatd-sema-sanctions', country+'-'+type+'-'+id)

    if country is not None:
        nationality = entity.create_nationality()
        nationality.country = country

    if type is 'ind':
        parse_individual(entity, node)
        parse_dob(entity, node)
    else:
        parse_entity(entity, node)

    parse_alias(entity, node)
    parse_schedule(entity, node)

    # pprint(entity.to_dict())
    context.emit(data=entity.to_dict())


def parse_individual(entity, node):
    entity.type = Entity.TYPE_INDIVIDUAL
    entity.first_name = node.findtext('.//GivenName')
    entity.last_name = node.findtext('.//LastName')


def parse_entity(entity, node):
    entity.type = Entity.TYPE_ENTITY

    # Some entities have French translations
    names = node.findtext('.//Entity').split('/')
    entity.name = names.pop(0)

    # Save them as aliases
    if len(names) > 0:
        for name in names:
            entity.create_alias(name=name)


def parse_dob(entity, node):
    dob = node.findtext('.//DateOfBirth')
    if dob is None:
        return

    birth_date = entity.create_birth_date()

    if '/' not in dob:
        birth_date.date = dob
        birth_date.quality = BirthDate.QUALITY_WEAK
    else:
        day, month, year = dob.split('/', 2)
        birth_date.date = year+'-'+month+'-'+day
        birth_date.quality = BirthDate.QUALITY_STRONG


def get_country(node):
    names = node.findtext('.//Country')
    if names is None:
        return None

    # Only keep english version
    name = names.split(' / ')[0]
    name = collapse_spaces(name)
    if not len(name):
        return None

    return name


def parse_alias(entity, node):
    names = node.findtext('.//Aliases')
    if names is None:
        return

    for name in names.split(', '):
        name = collapse_spaces(name)
        if not len(name):
            continue

        # Some aliases have multiple parts
        parts = name.split('/')
        for part in parts:
            entity.create_alias(name=part)


def parse_schedule(entity, node):
    schedule = node.findtext('.//Schedule')
    if schedule is None or schedule is 'N/A':
        return
    entity.summary = 'Schedule '+schedule
