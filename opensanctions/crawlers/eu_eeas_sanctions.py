from pprint import pprint  # noqa

from normality import stringify
from memorious.helpers import parse_date

from opensanctions.models import Entity, Identifier


ENTITY_TYPES = {
    'P': Entity.TYPE_INDIVIDUAL,
    'E': Entity.TYPE_ENTITY
}
GENDERS = {
    'M': Entity.GENDER_MALE,
    'F': Entity.GENDER_FEMALE,
    '': None,
    None: None
}


def parse_entry(context, entry):
    type_ = ENTITY_TYPES[entry.get('Type')]
    entity = Entity.create('eu-eeas-sanctions', entry.get('Id'))
    entity.type = type_
    entity.updated_at = entry.get('reg_date')
    entity.url = entry.get('pdf_link')
    entity.program = entry.get('programme')
    entity.summary = entry.get('remark')

    for name in entry.findall('./NAME'):
        if entity.name is None:
            obj = entity
        else:
            obj = entity.create_alias()

        obj.title = name.findtext('./TITLE')
        obj.name = name.findtext('./WHOLENAME')
        obj.first_name = name.findtext('./FIRSTNAME')
        obj.second_name = name.findtext('./MIDDLENAME')
        obj.last_name = name.findtext('./LASTNAME')

        if entity.function is None:
            entity.function = name.findtext('./FUNCTION')

        if entity.gender is None:
            entity.gender = GENDERS[name.findtext('./GENDER')]

    for passport in entry.findall('./PASSPORT'):
        identifier = entity.create_identifier()
        identifier.type = Identifier.TYPE_PASSPORT
        identifier.number = passport.findtext('./NUMBER')
        identifier.country = passport.findtext('./COUNTRY')

    for node in entry.findall('./ADDRESS'):
        address = entity.create_address()
        address.street = node.findtext('./STREET')
        address.street_2 = node.findtext('./NUMBER')
        address.city = node.findtext('./CITY')
        address.postal_code = node.findtext('./ZIPCODE')
        address.country = node.findtext('./COUNTRY')

    for birth in entry.findall('./BIRTH'):
        place = stringify(birth.findtext('./PLACE'))
        country = stringify(birth.findtext('./COUNTRY'))
        if place is not None or country is not None:
            birth_place = entity.create_birth_place()
            birth_place.place = place
            birth_place.country = country

        date_ = stringify(parse_date(birth.findtext('./DATE')))
        if date_ is not None:
            birth_date = entity.create_birth_date()
            birth_date.date = date_

    for country in entry.findall('./CITIZEN/COUNTRY'):
        nationality = entity.create_nationality()
        nationality.country = country.text

    # pprint(entity.to_dict())
    context.emit(data=entity.to_dict())


def eeas_parse(context, data):
    res = context.http.rehash(data)
    doc = res.xml
    for entry in doc.findall('.//ENTITY'):
        parse_entry(context, entry)
