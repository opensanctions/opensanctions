from lxml import etree
from dateutil.parser import parse as dateutil_parse
from datetime import datetime

from pepparser.util import remove_namespace, make_id
from pepparser.text import combine_name
from pepparser.country import normalize_country


BASE = {
    'publisher': 'European External Action Service',
    'publisher_url': 'http://eeas.europa.eu/',
    'source': 'Consolidated list of entities subject to EU financial sanctions',
    'source_url': 'http://eeas.europa.eu/cfsp/sanctions/consol-list/index_en.htm'
}


def parse_date(date):
    if date is None and len(date.strip()):
        return
    try:
        return dateutil_parse(date).date().isoformat()
    except:
        return


def parse_entry(emit, record, entry):
    uid = entry.get('Id')
    record.update({
        'uid': make_id('eu', 'eeas', uid),
        'type': 'individual',
        'updated_at': entry.get('reg_date'),
        'source_url': entry.get('pdf_link'),
        'program': entry.get('programme'),
        'summary': entry.get('remark'),
        # 'first_name': entry.findtext('./firstName'),
        # 'last_name': entry.findtext('./lastName'),
        # 'name': combine_name(entry.findtext('./firstName'),
        #                      entry.findtext('./lastName'))
    })
    is_entity = entry.get('Type') != 'P'
    if is_entity:
        record['type'] = 'entity'

    record['other_names'] = []
    for aka in entry.findall('./NAME'):
        data = {
            'first_name': aka.findtext('./FIRSTNAME'),
            'last_name': aka.findtext('./LASTNAME'),
            'middle_name': aka.findtext('./MIDDLENAME'),
            'other_name': aka.findtext('./WHOLENAME')
        }

        funct = aka.findtext('./FUNCTION')
        if funct and len(funct) > len(data.get('function', '')):
            data['function'] = funct

        gender = aka.findtext('./GENDER')
        if gender == 'M':
            data['gender'] = 'male'
        if gender == 'F':
            data['gender'] = 'female'

        if 'name' not in record:
            record['name'] = data.pop('other_name')
            record.update(data)
        else:
            record['other_names'].append(data)

    record['identities'] = []
    for passport in entry.findall('./PASSPORT'):
        data = {
            'number': passport.findtext('./NUMBER'),
            'country': normalize_country(passport.findtext('./COUNTRY'))
        }
        record['identities'].append(data)

    record['addresses'] = []
    for address in entry.findall('./ADDRESS'):
        data = {
            'address1': address.findtext('./STREET'),
            'address2': address.findtext('./NUMBER'),
            'city': address.findtext('./CITY'),
            'postal_code': address.findtext('./ZIPCODE'),
            'country': normalize_country(address.findtext('./COUNTRY'))
        }
        record['addresses'].append(data)

    for birth in entry.findall('./BIRTH'):
        place = birth.findtext('./PLACE')
        if place and len(place) > len(record.get('place_of_birth', '')):
            record['place_of_birth'] = place
        date_ = parse_date(birth.findtext('./DATE'))
        if date_ and len(date_) > len(record.get('date_of_birth', '')):
            record['date_of_birth'] = date_

        # TODO:
        country_ = birth.findtext('./COUNTRY')

    # print etree.tostring(entry, pretty_print=True)
    emit.entity(record)


def eeas_parse(emit, xmlfile):
    doc = etree.parse(xmlfile)
    for entry in doc.findall('.//ENTITY'):
        parse_entry(emit, BASE.copy(), entry)
