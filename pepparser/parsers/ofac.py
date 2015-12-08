from lxml import etree
from dateutil.parser import parse as dateutil_parse
from datetime import datetime

from pepparser.util import remove_namespace, make_id
from pepparser.text import combine_name
from pepparser.country import normalize_country


PUBLISHER = {
    'publisher': 'US Treasury Office of Foreign Assets Control',
    'publisher_url': 'https://www.treasury.gov/about/organizational-structure/offices/Pages/Office-of-Foreign-Assets-Control.aspx',    
    'source_url': 'http://sdnsearch.ofac.treas.gov/'
}


def parse_date(date):
    if date is None or not len(date.strip()):
        return
    try:
        return dateutil_parse(date).date().isoformat()
    except:
        return


def parse_entry(emit, record, entry):
    uid = entry.findtext('uid')
    record.update({
        'uid': make_id('us', 'ofac', uid),
        'type': 'individual',
        'program': entry.findtext('./programList/program'),
        'summary': entry.findtext('./remarks'),
        'first_name': entry.findtext('./firstName'),
        'last_name': entry.findtext('./lastName'),
        'name': combine_name(entry.findtext('./firstName'),
                             entry.findtext('./lastName'))
    })
    is_entity = entry.findtext('./sdnType') != 'Individual'
    if is_entity:
        record['type'] = 'entity'
        record.pop('last_name', None)

    record['other_names'] = []
    for aka in entry.findall('./akaList/aka'):
        data = {
            'type': aka.findtext('./type'),
            'quality': aka.findtext('./category'),
            'first_name': aka.findtext('./firstName'),
            'last_name': aka.findtext('./lastName'),
            'other_name': combine_name(aka.findtext('./firstName'),
                                       aka.findtext('./lastName'))
        }
        if is_entity:
            data.pop('last_name', None)
        record['other_names'].append(data)

    record['identities'] = []
    for ident in entry.findall('./idList/id'):
        data = {
            'type': ident.findtext('./idType'),
            'number': ident.findtext('./idNumber'),
            'country': normalize_country(ident.findtext('./idCountry'))
        }
        record['identities'].append(data)

    record['addresses'] = []
    for address in entry.findall('./addressList/address'):
        data = {
            'address1': address.findtext('./address1'),
            'address2': address.findtext('./address2'),
            'city': address.findtext('./city'),
            'country': normalize_country(address.findtext('./country'))
        }
        record['addresses'].append(data)

    for pob in entry.findall('./placeOfBirthList/placeOfBirthItem'):
        if pob.findtext('./mainEntry') == 'true':
            record['place_of_birth'] = pob.findtext('./placeOfBirth')

    for pob in entry.findall('./dateOfBirthList/dateOfBirthItem'):
        if pob.findtext('./mainEntry') == 'true':
            dt = pob.findtext('./dateOfBirth')
            record['date_of_birth'] = parse_date(dt)

    # print etree.tostring(entry, pretty_print=True)

    if is_entity:
        record.pop('last_name', None)
    emit.entity(record)


def ofac_parse(emit, sdn, consolidated, xmlfile):
    doc = etree.parse(xmlfile)
    remove_namespace(doc, 'http://tempuri.org/sdnList.xsd')

    publish_date = datetime.strptime(doc.findtext('.//Publish_Date'),
                                     '%m/%d/%Y')
    source = PUBLISHER.copy()
    source['updated_at'] = publish_date.date().isoformat()
    if sdn:
        source['source'] = 'Specially Designated Nationals and Blocked Persons'
        source['source_id'] = 'US-OFAC-SDN'
    if consolidated:
        source['source'] = 'Consolidated non-SDN List'
        source['source_id'] = 'US-OFAC-NONSDN'

    for entry in doc.findall('.//sdnEntry'):
        record = source.copy()
        parse_entry(emit, record, entry)
