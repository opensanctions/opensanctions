from pprint import pprint  # noqa
from normality import collapse_spaces, stringify

from opensanctions.util import EntityEmitter, normalize_country


def values(node):
    if node is None:
        return []
    return [c.text for c in node.findall('./VALUE')]


def parse_alias(entity, node):
    names = node.findtext('./ALIAS_NAME')
    quality = node.findtext('./QUALITY')
    if names is None:
        return

    for name in names.split('; '):
        name = collapse_spaces(name)
        if not len(name):
            continue

        if quality == 'Low':
            entity.add('weakAlias', name)
        elif quality == 'Good':
            entity.add('alias', name)
        elif quality == 'a.k.a.':
            entity.add('alias', name)
        elif quality == 'f.k.a.':
            entity.add('previousName', name)


def parse_address(entity, addr):
    text = addr.xpath('string()').strip()
    if not len(text):
        return
    note = addr.findtext('./NOTE')
    street = addr.findtext('./STREET')
    city = addr.findtext('./CITY')
    region = addr.findtext('./STATE_PROVINCE')
    country = addr.findtext('./COUNTRY')
    parts = (note, street, city, region, country)
    parts = [p for p in parts if stringify(p) is not None]
    entity.add('address', ', '.join(parts))
    entity.add('country', normalize_country(country))


def parse_entity(emitter, node):
    entity = emitter.make('LegalEntity')
    sanction = parse_common(emitter, entity, node)
    entity.add('alias', node.findtext('./FIRST_NAME'))

    for alias in node.findall('./ENTITY_ALIAS'):
        parse_alias(entity, alias)

    for addr in node.findall('./ENTITY_ADDRESS'):
        parse_address(entity, addr)

    emitter.emit(entity)
    emitter.emit(sanction)


def parse_individual(emitter, node):
    person = emitter.make('Person')
    sanction = parse_common(emitter, person, node)
    person.add('title', values(node.find('./TITLE')))
    person.add('firstName', node.findtext('./FIRST_NAME'))
    person.add('secondName', node.findtext('./SECOND_NAME'))
    person.add('middleName', node.findtext('./THIRD_NAME'))
    person.add('position', values(node.find('./DESIGNATION')))

    for alias in node.findall('./INDIVIDUAL_ALIAS'):
        parse_alias(person, alias)

    for addr in node.findall('./INDIVIDUAL_ADDRESS'):
        parse_address(person, addr)

    for doc in node.findall('./INDIVIDUAL_DOCUMENT'):
        passport = emitter.make('Passport')
        number = doc.findtext('./NUMBER')
        date = doc.findtext('./DATE_OF_ISSUE')
        type_ = doc.findtext('./TYPE_OF_DOCUMENT')
        if number is None and date is None and type_ is None:
            continue
        passport.make_id(person.id, number, date, type_)
        passport.add('holder', person)
        passport.add('passportNumber', number)
        passport.add('startDate', date)
        passport.add('type', type_)
        passport.add('type', doc.findtext('./TYPE_OF_DOCUMENT2'))
        passport.add('summary', doc.findtext('./NOTE'))
        country = doc.findtext('./COUNTRY_OF_ISSUE')
        country = country or doc.findtext('./ISSUING_COUNTRY')
        passport.add('country', normalize_country(country))
        emitter.emit(passport)

    for nat in node.findall('./NATIONALITY/VALUE'):
        person.add('nationality', normalize_country(nat.text))

    for dob in node.findall('./INDIVIDUAL_DATE_OF_BIRTH'):
        date = dob.findtext('./DATE') or dob.findtext('./YEAR')
        person.add('birthDate', date)

    for pob in node.findall('./INDIVIDUAL_PLACE_OF_BIRTH'):
        person.add('country', normalize_country(pob.findtext('./COUNTRY')))
        place = (pob.findtext('./CITY'),
                 pob.findtext('./STATE_PROVINCE'),
                 pob.findtext('./COUNTRY'))
        place = [p for p in place if stringify(p) is not None]
        person.add('birthPlace', ', '.join(place))

    emitter.emit(person)
    emitter.emit(sanction)


def parse_common(emitter, entity, node):
    entity.make_id(node.findtext('./DATAID'))
    name = node.findtext('./NAME_ORIGINAL_SCRIPT')
    name = name or node.findtext('./FIRST_NAME')
    entity.add('name', name)
    entity.add('description', node.findtext('./COMMENTS1'))
    entity.add('modifiedAt', values(node.find('./LAST_DAY_UPDATED')))

    sanction = emitter.make('Sanction')
    sanction.make_id(entity.id)
    sanction.add('entity', entity)
    sanction.add('authority', 'United Nations Security Council')
    sanction.add('startDate', node.findtext('./LISTED_ON'))
    sanction.add('modifiedAt', values(node.find('./LAST_DAY_UPDATED')))

    program = '%s (%s)' % (node.findtext('./UN_LIST_TYPE').strip(),
                           node.findtext('./REFERENCE_NUMBER').strip())
    sanction.add('program', program)
    return sanction


def parse(context, data):
    emitter = EntityEmitter(context)
    with context.http.rehash(data) as res:
        for node in res.xml.findall('.//INDIVIDUAL'):
            parse_individual(emitter, node)

        for node in res.xml.findall('.//ENTITY'):
            parse_entity(emitter, node)
