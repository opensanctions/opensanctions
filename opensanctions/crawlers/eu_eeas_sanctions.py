from pprint import pprint  # noqa

from opensanctions import constants
from opensanctions.util import EntityEmitter
from opensanctions.util import jointext

GENDERS = {
    'M': constants.MALE,
    'F': constants.FEMALE
}


def parse_entry(emitter, entry):
    entity = emitter.make('LegalEntity')
    if entry.get('Type') == 'P':
        entity = emitter.make('Person')
    entity.make_id(entry.get('Id'))
    entity.add('sourceUrl', entry.get('pdf_link'))

    sanction = emitter.make('Sanction')
    sanction.make_id(entity.id)
    sanction.add('entity', entity)
    sanction.add('authority', 'European External Action Service')
    sanction.add('sourceUrl', entry.get('pdf_link'))
    program = jointext(entry.get('programme'), entry.get('legal_basis'),
                       sep=' - ')
    sanction.add('program', program)
    sanction.add('reason', entry.get('remark'))
    sanction.add('startDate', entry.get('reg_date'))

    for name in entry.findall('./NAME'):
        if entity.has('name'):
            entity.add('alias', name.findtext('./WHOLENAME'))
        else:
            entity.add('name', name.findtext('./WHOLENAME'))
        entity.add('title', name.findtext('./TITLE'), quiet=True)
        entity.add('firstName', name.findtext('./FIRSTNAME'), quiet=True)
        entity.add('middleName', name.findtext('./MIDDLENAME'), quiet=True)
        entity.add('lastName', name.findtext('./LASTNAME'), quiet=True)
        entity.add('position', name.findtext('./FUNCTION'), quiet=True)
        gender = GENDERS.get(name.findtext('./GENDER'))
        entity.add('gender', gender, quiet=True)

    for pnode in entry.findall('./PASSPORT'):
        passport = emitter.make('Passport')
        passport.make_id('Passport', entity.id, pnode.findtext('./NUMBER'))
        passport.add('holder', entity)
        passport.add('passportNumber', pnode.findtext('./NUMBER'))
        passport.add('country', pnode.findtext('./COUNTRY'))
        emitter.emit(passport)

    for node in entry.findall('./ADDRESS'):
        address = jointext(node.findtext('./STREET'),
                           node.findtext('./NUMBER'),
                           node.findtext('./CITY'),
                           node.findtext('./ZIPCODE'))
        entity.add('address', address)
        entity.add('country', node.findtext('./COUNTRY'))

    for birth in entry.findall('./BIRTH'):
        entity.add('birthDate', birth.findtext('./DATE'))
        entity.add('birthPlace', birth.findtext('./PLACE'))
        entity.add('country', birth.findtext('./COUNTRY'))

    for country in entry.findall('./CITIZEN/COUNTRY'):
        entity.add('nationality', country.text, quiet=True)

    emitter.emit(entity)
    emitter.emit(sanction)


def eeas_parse(context, data):
    emitter = EntityEmitter(context)
    with context.http.rehash(data) as res:
        for entry in res.xml.findall('.//ENTITY'):
            parse_entry(emitter, entry)
    emitter.finalize()
