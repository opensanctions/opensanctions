from lxml import etree
from dateutil.parser import parse as dateutil_parse
from datetime import datetime

from pepparser.util import remove_namespace, make_id
from pepparser.text import combine_name
from pepparser.country import normalize_country


BASE = {
    'publisher': 'United Nations Security Council',
    'publisher_url': 'https://www.un.org/en/sc/',
    'source': 'Consolidated Sanctions List',
    'source_url': 'https://www.un.org/sc/suborg/sites/www.un.org.sc.suborg/files/consolidated.htm'
}


def parse_alias(alias, record):
    names = alias.findtext('./ALIAS_NAME')
    if names is None:
        return

    for name in names.split('; '):
        if not len(name.strip()):
            continue

        record['other_names'].append({
            'quality': alias.findtext('./QUALITY'),
            'other_name': name,
        })


def parse_address(addr, record):
    data = {
        'text': addr.findtext('./NOTE'),
        'address1': addr.findtext('./STREET'),
        'city': addr.findtext('./CITY'),
        'region': addr.findtext('./STATE_PROVINCE'),
        'country': normalize_country(addr.findtext('./COUNTRY')),
    }
    exist = set(data.values())
    if exist == 1 and exist[0] is None:
        return
    record['addresses'].append(data)


def parse_entity(emit, record, ent):
    for alias in ent.findall('./ENTITY_ALIAS'):
        parse_alias(alias, record)

    for addr in ent.findall('./ENTITY_ADDRESS'):
        parse_address(addr, record)

    emit.entity(record)


def parse_individual(emit, record, ind):
    record.update({
        'first_name': ind.findtext('.//FIRST_NAME'),
        'last_name': ind.findtext('.//LAST_NAME')
    })

    for alias in ind.findall('./INDIVIDUAL_ALIAS'):
        parse_alias(alias, record)

    for addr in ind.findall('./INDIVIDUAL_ADDRESS'):
        parse_address(addr, record)

    for ident in ind.findall('./INDIVIDUAL_DOCUMENT'):
        country = normalize_country(ident.findtext('./COUNTRY_OF_ISSUE'))
        number = ident.findtext('./NUMBER')
        if number is None and country is None:
            continue
        record['identities'].append({
            'type': ident.findtext('./TYPE_OF_DOCUMENT'),
            'number': number,
            'country': country,
        })

    for dob in ind.findall('./INDIVIDUAL_DATE_OF_BIRTH'):
        date = dob.findtext('./DATE')
        if date is None:
            continue
        if ':' in date:
            date = date.rsplit('-', 1)[0]
        approx = dob.findtext('./TYPE_OF_DATE') == 'Approximately'
        if approx and 'date_of_birth' in record:
            continue
        record['date_of_birth'] = date

    for pob in ind.findall('./INDIVIDUAL_PLACE_OF_BIRTH'):
        country = pob.findtext('./COUNTRY')
        record['country_of_birth'] = normalize_country(country)

        place = pob.findtext('./CITY')
        region = pob.findtext('./STATE_PROVINCE')
        if place and region:
            record['place_of_birth'] = '%s (%s)' % (place, region)
        if place:
            record['place_of_birth'] = place
        if region:
            record['place_of_birth'] = region

    emit.entity(record)


def parse_common(node, type_):
    program_ref = '%s (%s)' % (node.findtext('./UN_LIST_TYPE').strip(),
                               node.findtext('./REFERENCE_NUMBER').strip())
    record = {
        'uid': make_id('un', 'sc', node.findtext('./DATAID')),
        'type': type_,
        'program': program_ref,
        'summary': node.findtext('./COMMENTS1'),
        'name': combine_name(node.findtext('./FIRST_NAME'),
                             node.findtext('./LAST_NAME')),
        'function': node.findtext('./DESIGNATION/VALUE'),
        'updated_at': node.findtext('./LISTED_ON'),
        'nationality': normalize_country(node.findtext('./NATIONALITY/VALUE')),
        'other_names': [],
        'addresses': [],
        'identities': []
    }
    record.update(BASE)
    orig = node.findtext('./NAME_ORIGINAL_SCRIPT')
    if orig is not None:
        record['name'] = orig

    last_updated = node.findtext('./LAST_DAY_UPDATED/VALUE')
    if last_updated is not None:
        record['updated_at'] = last_updated

    if ':' in record['updated_at']:
        record['updated_at'] = record['updated_at'].rsplit('-', 1)[0]

    # print etree.tostring(node, pretty_print=True)
    return record


def unsc_parse(emit, xmlfile):
    doc = etree.parse(xmlfile)

    for node in doc.findall('.//INDIVIDUAL'):
        record = parse_common(node, 'individual')
        parse_individual(emit, record, node)

    for node in doc.findall('.//ENTITY'):
        record = parse_common(node, 'entity')
        parse_entity(emit, record, node)
