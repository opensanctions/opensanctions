from datetime import date
from pprint import pprint  # noqa

from opensanctions.models import Entity, Alias, Identifier


ID_TYPES = {
    'diplomatic-passport': Identifier.TYPE_PASSPORT,
    'driving-license': Identifier.TYPE_OTHER,
    'driving-permit': Identifier.TYPE_OTHER,
    'id-card': Identifier.TYPE_NATIONALID,
    'other': Identifier.TYPE_OTHER,
    'passport': Identifier.TYPE_PASSPORT,
    'resident-permit': Identifier.TYPE_OTHER,
    'residence-permit': Identifier.TYPE_OTHER,
    'travel-document': Identifier.TYPE_PASSPORT
}

QUALITY = {
    'good': Alias.QUALITY_STRONG,
    'low': Alias.QUALITY_WEAK
}

NAME_PARTS = {
    'family-name': 'last_name',
    'given-name': 'first_name',
    'further-given-name': 'second_name',
    'father-name': 'father_name',
    'whole-name': 'name',
    'title': 'title',
    'grand-father-name': 'third_name',
    'tribal-name': 'last_name',
    'maiden-name': None,
    'other': None,
    'suffix': None,
}


def parse_date(el):
    if el is None or not el.get('year'):
        return
    if el.get('month') and el.get('day'):
        try:
            return date(int(el.get('year')),
                        int(el.get('month')),
                        int(el.get('day'))).isoformat()
        except ValueError:
            return date(int(el.get('year')),
                        int(el.get('month')),
                        int(el.get('day')) - 1).isoformat()
    else:
        return el.get('year')


def parse_address(entity, node, places):
    if not len(node.getchildren()):
        node = places.get(node.get('place-id'))
    address = entity.create_address()
    address.text = node.findtext('./location') or node.findtext('./c-o')
    address.note = node.findtext('./remarks')
    address.street = node.findtext('./address-details')
    address.street_2 = node.findtext('./p-o-box')
    address.postal_code = node.findtext('./zip-code')
    address.region = node.findtext('./area')

    country = node.find('./country')
    if country is not None:
        address.country = country.text
        if country.get('iso-code'):
            address.country_code = country.get('iso-code')


def parse_name(entity, node, main):
    quality = QUALITY[node.get('quality')]
    obj = entity
    if 'alias' == node.get('name-type') or not main:
        obj = entity.create_alias()
        obj.quality = quality

    variants = {}
    for part_node in node.findall('./name-part'):
        part_type = NAME_PARTS[part_node.get('name-part-type')]
        if part_type is None:
            continue

        value = part_node.findtext('./value')
        setattr(obj, part_type, value)

        for spelling in part_node.findall('./spelling-variant'):
            key = (spelling.get('lang'), spelling.get('script'))
            if key not in variants:
                variants[key] = entity.create_alias()
            setattr(variants[key], part_type, spelling.text)


def parse_identity(entity, node, places):
    main = node.get('main') == 'true'

    for name_node in node.findall('./name'):
        parse_name(entity, name_node, main)

    for address_node in node.findall('./address'):
        parse_address(entity, address_node, places)

    for bday_node in node.findall('./day-month-year'):
        birth_date = entity.create_birth_date()
        birth_date.date = parse_date(bday_node)
        birth_date.quality = QUALITY[bday_node.get('quality')]

    for place_node in node.findall('./place-of-birth'):
        res_place = places.get(place_node.get('place-id'))
        if place_node is None:
            continue

        birth_place = entity.create_birth_place()
        birth_place.quality = QUALITY[place_node.get('quality')]
        birth_place.place = res_place.findtext('./location')
        area = res_place.findtext('./area')
        if area is not None:
            if birth_place.place is not None:
                birth_place.place = '%s; %s' % (birth_place.place, area)
            else:
                birth_place.place = area

        country = res_place.find('./country')
        if country is not None:
            birth_place.country = country.text
            if country.get('iso-code') is not None:
                birth_place.country_code = country.get('iso-code')

    for doc in node.findall('./identification-document'):
        identifier = entity.create_identifier()
        identifier.number = doc.findtext('./number')
        identifier.description = doc.get('document-type')
        identifier.type = ID_TYPES[identifier.description]
        country = doc.find('./issuer')
        if country is not None:
            identifier.country = country.text
            if country.get('code'):
                identifier.country_code = country.get('code')


def parse_entry(context, target, updated_at, sanctions, places):
    node = target.find('./individual')
    type_ = Entity.TYPE_INDIVIDUAL
    if node is None:
        node = target.find('./entity')
        type_ = Entity.TYPE_ENTITY
    if node is None:
        # node = target.find('./object')
        # TODO: build out support for these!
        return

    entity = Entity.create('ch-seco-sanctions', target.get('ssid'))
    entity.type = type_
    entity.updated_at = updated_at
    entity.program = sanctions.get(target.get('sanctions-set-id'))
    entity.function = node.findtext('./other-information')
    entity.summary = node.findtext('./justification')

    for inode in node.findall('./identity'):
        parse_identity(entity, inode, places)

    # pprint(entity.to_dict())
    context.emit(data=entity.to_dict())


def seco_parse(context, data):
    res = context.http.rehash(data)
    doc = res.xml
    updated_at = doc.getroot().get('date')

    programs = {}
    for sanc in doc.findall('.//sanctions-program'):
        key = sanc.find('./sanctions-set').get('ssid')
        label = sanc.findtext('./program-name[@lang="eng"]')
        programs[key] = label

    places = {}
    for place in doc.findall('.//place'):
        places[place.get('ssid')] = place

    for target in doc.findall('./target'):
        parse_entry(context, target, updated_at, programs, places)
