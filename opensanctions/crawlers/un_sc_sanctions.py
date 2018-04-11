from pprint import pprint  # noqa

from normality import collapse_spaces

from opensanctions.models import Entity, Identifier, Alias, BirthDate


ID_TYPES = {
    'Passport': Identifier.TYPE_PASSPORT,
    'National Identification Number': Identifier.TYPE_NATIONALID,
    u'Num\xe9ro de passeport ': Identifier.TYPE_PASSPORT,
    u'N\xfamero de pasaporte': Identifier.TYPE_PASSPORT,
    'National identification no.': Identifier.TYPE_NATIONALID,
    None: None
}

QUALITY = {
    'Low': Alias.QUALITY_WEAK,
    'Good': Alias.QUALITY_STRONG,
    'a.k.a.': Alias.QUALITY_STRONG,
    'f.k.a.': Alias.QUALITY_WEAK,
    'EXACT': BirthDate.QUALITY_STRONG,
    'APPROXIMATELY': BirthDate.QUALITY_WEAK
}


def parse_alias(entity, node):
    names = node.findtext('./ALIAS_NAME')
    if names is None:
        return

    for name in names.split('; '):
        name = collapse_spaces(name)
        if not len(name):
            continue

        alias = entity.create_alias(name=name)
        alias.quality = QUALITY[node.findtext('./QUALITY')]


def parse_address(entity, addr):
    text = addr.xpath('string()').strip()
    if not len(text):
        return
    address = entity.create_address()
    address.note = addr.findtext('./NOTE')
    address.street = addr.findtext('./STREET')
    address.city = addr.findtext('./CITY')
    address.region = addr.findtext('./STATE_PROVINCE')
    address.country = addr.findtext('./COUNTRY')


def parse_entity(context, node):
    entity = parse_common(node)
    entity.type = entity.TYPE_ENTITY

    for alias in node.findall('./ENTITY_ALIAS'):
        parse_alias(entity, alias)

    for addr in node.findall('./ENTITY_ADDRESS'):
        parse_address(entity, addr)

    # pprint(entity.to_dict())
    context.emit(data=entity.to_dict())


def parse_individual(context, node):
    entity = parse_common(node)
    entity.type = entity.TYPE_INDIVIDUAL
    entity.title = node.findtext('./TITLE/VALUE')
    entity.first_name = node.findtext('.//FIRST_NAME')
    entity.second_name = node.findtext('.//SECOND_NAME')
    entity.third_name = node.findtext('.//THIRD_NAME')

    for alias in node.findall('./INDIVIDUAL_ALIAS'):
        parse_alias(entity, alias)

    for addr in node.findall('./INDIVIDUAL_ADDRESS'):
        parse_address(entity, addr)

    for doc in node.findall('./INDIVIDUAL_DOCUMENT'):
        if doc.findtext('./NUMBER') is None:
            continue
        identifier = entity.create_identifier()
        identifier.country = doc.findtext('./COUNTRY_OF_ISSUE')
        if identifier.country is None:
            identifier.country = doc.findtext('./ISSUING_COUNTRY')
        identifier.number = doc.findtext('./NUMBER')
        identifier.type = ID_TYPES[doc.findtext('./TYPE_OF_DOCUMENT')]
        identifier.description = doc.findtext('./NOTE')
        identifier.issued_at = doc.findtext('./DATE_OF_ISSUE')

    for nat in node.findall('./NATIONALITY/VALUE'):
        nationality = entity.create_nationality()
        nationality.country = nat.text

    for dob in node.findall('./INDIVIDUAL_DATE_OF_BIRTH'):
        date = dob.findtext('./DATE') or dob.findtext('./YEAR')
        if date is None:
            continue
        birth_date = entity.create_birth_date()
        birth_date.quality = QUALITY[dob.findtext('./TYPE_OF_DATE')]
        birth_date.date = date

    for pob in node.findall('./INDIVIDUAL_PLACE_OF_BIRTH'):
        place = pob.findtext('./CITY')
        region = pob.findtext('./STATE_PROVINCE')
        country = pob.findtext('./COUNTRY')
        if place is None and region is None and country is None:
            continue
        birth_place = entity.create_birth_place()
        birth_place.place = place
        if region is not None:
            if place is not None:
                birth_place.place = '%s (%s)' % (place, region)
            else:
                birth_place.place = region
        birth_place.country = country

    # pprint(entity.to_dict())
    context.emit(data=entity.to_dict())


def parse_common(node):
    entity = Entity.create('un-sc-sanctions', node.findtext('./DATAID'))
    entity.program = '%s (%s)' % (node.findtext('./UN_LIST_TYPE').strip(),
                                  node.findtext('./REFERENCE_NUMBER').strip())
    entity.summary = node.findtext('./COMMENTS1')
    entity.function = node.findtext('./DESIGNATION/VALUE')
    entity.listed_at = node.findtext('./LISTED_ON')
    entity.updated_at = node.findtext('./LAST_DAY_UPDATED/VALUE')
    entity.name = node.findtext('./NAME_ORIGINAL_SCRIPT')
    entity.first_name = node.findtext('./FIRST_NAME')
    entity.second_name = node.findtext('./SECOND_NAME')
    entity.third_name = node.findtext('./THIRD_NAME')
    return entity


def parse(context, data):
    res = context.http.rehash(data)
    doc = res.xml

    for node in doc.findall('.//INDIVIDUAL'):
        parse_individual(context, node)

    for node in doc.findall('.//ENTITY'):
        parse_entity(context, node)
