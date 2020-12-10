from pprint import pprint  # noqa
from ftmstore.memorious import EntityEmitter
import requests
from opensanctions import constants
import time

SEXES = {
    "M": constants.MALE,
    "F": constants.FEMALE,
}

NATURES = {
    'E': 'Corporation',
    'P': 'Physical person'
}


def parse_entity(emitter, data: dict):
    _entity = emitter.make("Person")

    _entity.id = ("{}{}{}".format(data.pop('Date_file'), data.pop(
        'Naal_logical_id'), data.pop('EU_ref_num')
    ))
    _entity.add('status', NATURES.get(data.pop('Subject_type')))
    _entity.add('name', data.pop('Naal_wholename'))

    first_name = data.pop('Naal_firstname')
    _entity.add('firstName', first_name if first_name != '' else 'None')

    last_name = data.pop('Naal_lastname')
    _entity.add('lastName', last_name if last_name != '' else 'None')

    birth_date = data.pop('Birt_date')
    _entity.add('birthDate', birth_date if birth_date != '' else 'None')

    gender = data.pop('Naal_gender')
    _entity.add('gender', SEXES.get(gender) if gender != '' else 'None')

    _entity.add('program', data.pop('Naal_programme'))
    _entity.add('notes', data.pop('Entity_remark'))
    _entity.add('publisherUrl', data.pop('Naal_leba_url'))
    _entity.add('retrievedAt', data.pop('Naal_leba_publication_date'))

    emitter.emit(_entity)


def parse(context, data):
    emitter = EntityEmitter(context)
    with context.http.rehash(data) as res:
        doc = res.xml
        item = doc.find('//item/[title=\'CSV - v1.0\']')
        response = requests.get(item.find('link').text)
        content = response.content.decode('utf-8').split('\r\n')
        headers = content[0][1::].split(';')
        for row in content[1:-1]:
            entity = dict(zip(headers, row.split(';')))
            parse_entity(emitter, entity)
    emitter.finalize()
