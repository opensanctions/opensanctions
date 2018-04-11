import re
from dateutil.parser import parse as dateutil_parse
from datetime import datetime
from pprint import pprint  # noqa

from opensanctions.models import Entity, Alias, Identifier

XML_URL = 'http://www.sdfm.gov.ua/content/file/Site_docs/Black_list/zBlackListFull.xml'  # noqa
ENTITY_TYPES = {
    '1': Entity.TYPE_ENTITY,
    '2': Entity.TYPE_INDIVIDUAL
}
ALIAS_QUALITY = {
    '1': Alias.QUALITY_STRONG,
    '2': Alias.QUALITY_WEAK,
    None: None
}


def parse_date(context, date):
    if date is None:
        return
    date = date.replace('.', '').strip()
    if ';' in date:
        date, _ = date.split(';', 1)
    try:
        return dateutil_parse(date).date().isoformat()
    except Exception as ex:
        match = re.search('\d{4}', date)
        if match:
            return match.group(0)
        context.log.exception(ex)


def parse_entry(context, entry):
    uid = entry.findtext('number-entry')
    entity = Entity.create('ua-sdfm-blacklist', uid)
    entity.type = ENTITY_TYPES[entry.findtext('./type-entry')]
    entity.program = entry.findtext('./program-entry')
    entity.summary = entry.findtext('./comments')
    entity.url = 'http://www.sdfm.gov.ua/articles.php?cat_id=87&lang=en'
    date_entry = entry.findtext('./date-entry')
    if date_entry:
        date_entry = datetime.strptime(date_entry, '%Y%m%d')
        entity.updated_at = date_entry.date().isoformat()

    for aka in entry.findall('./aka-list'):
        if aka.findtext('type-aka') == 'N':
            obj = entity
        else:
            obj = entity.create_alias()
            obj.type = aka.findtext('./category-aka')
            obj.description = aka.findtext('./type-aka')
            obj.quality = ALIAS_QUALITY[aka.findtext('./quality-aka')]
        obj.first_name = aka.findtext('./aka-name1')
        obj.second_name = aka.findtext('./aka-name2')
        obj.third_name = aka.findtext('./aka-name3')
        obj.last_name = aka.findtext('./aka-name4')

    for node in entry.findall('./title-list'):
        entity.title = node.text

    for doc in entry.findall('./document-list'):
        identifier = entity.create_identifier()
        identifier.type = Identifier.TYPE_PASSPORT
        identifier.description = doc.findtext('./document-reg')
        identifier.number = doc.findtext('./document-id')
        identifier.country = doc.findtext('./document-country')

    for doc in entry.findall('./id-number-list'):
        identifier = entity.create_identifier()
        identifier.type = Identifier.TYPE_NATIONALID
        identifier.description = doc.text

    for node in entry.findall('./address-list'):
        address = entity.create_address()
        address.text = node.findtext('./address')

    for pob in entry.findall('./place-of-birth-list'):
        birth_place = entity.create_birth_place()
        birth_place.place = pob.text

    for dob in entry.findall('./date-of-birth-list'):
        birth_date = entity.create_birth_date()
        birth_date.date = parse_date(context, dob.text)

    for nat in entry.findall('./nationality-list'):
        nationality = entity.create_nationality()
        nationality.country = nat.text

    # pprint(entity.to_dict())
    context.emit(data=entity.to_dict())


def parse(context, data):
    res = context.http.rehash(data)
    doc = res.xml

    for entry in doc.findall('.//acount-list'):
        parse_entry(context, entry)
