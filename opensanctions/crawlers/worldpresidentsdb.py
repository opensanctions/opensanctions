from pprint import pprint  # noqa

from memorious.helpers import make_id

from opensanctions.models import Entity


def parse_entry(context, entry):
    url = entry.get('href')
    res = context.http.get(url)
    doc = res.html
    content = doc.find('.//div[@id="content"]')

    uid = make_id(url)

    entity = Entity.create('worldpresidentsdb', uid)
    entity.type = Entity.TYPE_INDIVIDUAL
    entity.function = 'President'
    entity.url = url
    entity.first_name, entity.last_name = content.find('h1').text.split(' ', 1)

    for element in content.findall('.//p'):
        type = element.find('.//span')

        if type is None:
            continue
        else:
            type = type.text

        if type == 'Country:':
            nationality = entity.create_nationality()
            nationality.country = element.find('a').text
        elif type == 'Birth Date:':
            value = element[0].tail.strip()
            month, day, year = value.split('-', 2)
            birth_date = entity.create_birth_date()
            birth_date.date = year + '-' + month + '-' + day
            birth_date.quality = 'strong'
        elif type == 'Birth Place:':
            value = element[0].tail.strip()
            birth_place = entity.create_birth_place()
            birth_place.place = value
        elif type == 'Political Party:':
            value = element[0].tail.strip()
            entity.program = value
        elif type == 'Other Political Titles:':
            value = element[0].tail.strip()
            entity.summary = value
    # pprint(entity.to_dict())
    context.emit(data=entity.to_dict())


def parse(context, data):
    res = context.http.rehash(data)
    for member in res.html.findall('.//table[@id="list_table"]//td//a'):
        parse_entry(context, member)
