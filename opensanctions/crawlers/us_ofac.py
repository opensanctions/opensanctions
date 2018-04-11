from pprint import pprint  # noqa

from memorious.helpers import parse_date, make_id
from normality import stringify

from opensanctions.models import (
    Entity, Alias, Identifier, BirthPlace, BirthDate
)
from opensanctions.helpers import remove_namespace


ENTITY_TYPES = {
    'Individual': Entity.TYPE_INDIVIDUAL,
    'Entity': Entity.TYPE_ENTITY,
    'Vessel': Entity.TYPE_VESSEL,
    'Aircraft': None
}
ALIAS_QUALITY = {
    'strong': Alias.QUALITY_STRONG,
    'weak': Alias.QUALITY_WEAK
}
ID_TYPES = {
    'Passport': Identifier.TYPE_PASSPORT,
    'Additional Sanctions Information -': None,
    'US FEIN': Identifier.TYPE_OTHER,
    'SSN': Identifier.TYPE_OTHER,
    'Cedula No.': Identifier.TYPE_NATIONALID,
    'NIT #': Identifier.TYPE_NATIONALID,
}


def parse_entry(context, entry, url, updated_at):
    uid = entry.findtext('uid')
    type_ = ENTITY_TYPES[entry.findtext('./sdnType')]
    if type_ is None:
        return
    entity = Entity.create('us-ofac', make_id(url, uid))
    entity.type = type_
    entity.updated_at = updated_at
    programs = [p.text for p in entry.findall('./programList/program')]
    entity.program = '; '.join(programs)
    entity.summary = entry.findtext('./remarks')
    entity.function = entry.findtext('./title')
    entity.first_name = entry.findtext('./firstName')
    entity.last_name = entry.findtext('./lastName')

    for aka in entry.findall('./akaList/aka'):
        alias = entity.create_alias()
        alias.first_name = aka.findtext('./firstName')
        alias.last_name = aka.findtext('./lastName')
        alias.type = aka.findtext('./type')
        alias.quality = ALIAS_QUALITY[aka.findtext('./category')]

    for ident in entry.findall('./idList/id'):
        type_ = ID_TYPES.get(ident.findtext('./idType'), Identifier.TYPE_OTHER)
        if type_ is None:
            continue
        identifier = entity.create_identifier()
        identifier.type = type_
        identifier.number = ident.findtext('./idNumber')
        identifier.country = ident.findtext('./idCountry')
        identifier.description = ident.findtext('./idType')

    for addr in entry.findall('./addressList/address'):
        address = entity.create_address()
        address.street = addr.findtext('./address1')
        address.street_2 = addr.findtext('./address2')
        address.city = addr.findtext('./city')
        address.country = addr.findtext('./country')

    for pob in entry.findall('./placeOfBirthList/placeOfBirthItem'):
        birth_place = entity.create_birth_place()
        birth_place.place = pob.findtext('./placeOfBirth')
        birth_place.quality = BirthPlace.QUALITY_WEAK
        if pob.findtext('./mainEntry') == 'true':
            birth_place.quality = BirthPlace.QUALITY_STRONG

    for pob in entry.findall('./dateOfBirthList/dateOfBirthItem'):
        birth_date = entity.create_birth_date()
        birth_date.date = stringify(parse_date(pob.findtext('./dateOfBirth')))
        birth_date.quality = BirthDate.QUALITY_WEAK
        if pob.findtext('./mainEntry') == 'true':
            birth_date.quality = BirthDate.QUALITY_STRONG

    # pprint(entity.to_dict())
    context.emit(data=entity.to_dict())


def parse(context, data):
    urls = context.params.get('urls')
    for url in urls:
        res = context.http.get(url)
        doc = res.xml
        remove_namespace(doc, 'http://tempuri.org/sdnList.xsd')

        updated_at = doc.findtext('.//Publish_Date')
        updated_at = stringify(parse_date(updated_at, format_hint='%m/%d/%Y'))

        for entry in doc.findall('.//sdnEntry'):
            parse_entry(context, entry, url, updated_at)
