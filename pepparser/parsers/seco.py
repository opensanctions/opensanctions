from lxml import etree
from datetime import date
from itertools import combinations

from pepparser.util import make_id
from pepparser.text import combine_name
from pepparser.country import normalize_country


PUBLISHER = {
    'publisher': 'Swiss State Secretariat for Economic Affairs (SECO)',
    'publisher_url': 'http://www.seco.admin.ch/',
    'source': 'Sanctions / Embargoes',
    'source_url': 'http://www.seco.admin.ch/themen/00513/00620/04991/index.html?lang=en'
}


def parse_date(el):
    if el is None:
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


def parse_addr(addr, places):
    if not len(addr.getchildren()):
        addr = places.get(addr.get('place-id'))
    country = addr.find('./country')
    data = {
        'text': addr.findtext('./remarks') or addr.findtext('./location'),
        'address1': addr.findtext('./address-details'),
        'address2': addr.findtext('./p-o-box'),
        'postal_code': addr.findtext('./zip-code'),
        'address1': addr.findtext('./address-details'),
        'region': addr.findtext('./area'),
        'country': normalize_country(addr.findtext('./country'))
    }
    if country is not None and country.get('iso-code'):
        data['country'] = normalize_country(country.get('iso-code'))
    return data


def get_name_data(names):
    data = {}
    parts = []
    for (name_part, value) in names:
        np_type = name_part.get('name-part-type')

        if value and len(value) and value.strip() != '-':
            if np_type == 'whole-name':
                data['other_name'] = value
            if np_type == 'family-name':
                data['last_name'] = value
            if np_type == 'given-name':
                data['first_name'] = value
            if np_type == 'further-given-name':
                data['second_name'] = value
            parts.append((value, int(name_part.get('order'))))

    if 'other_name' not in data and len(parts):
        parts = sorted(parts, key=lambda (a, b): b)
        parts = [a for (a, b) in parts]
        data['other_name'] = combine_name(*parts)

    return data


def parse_name(name, record, main):
    primary = name.get('name-type') == 'primary-name'
    is_alias = not primary
    if 'name' in record and not main:
        is_alias = True

    names = []
    name_parts = name.findall('./name-part')
    for name_part in name_parts:
        value = name_part.findtext('./value')
        names.append((name_part, value))
    data = get_name_data(names)
    if is_alias:
        record['other_names'].append(data)
    else:
        record['name'] = data.pop('other_name')

    variants = []
    for name_part in name_parts:
        for var in name_part.findall('./spelling-variant'):
            variants.append((name_part, var))

    for name_cand in combinations(variants, len(name_parts)):
        langs = set([v.get('lang') for (np, v) in name_cand])
        if len(langs) > 1:
            continue
        scripts = set([v.get('script') for (np, v) in name_cand])
        if len(scripts) > 1:
            continue
        types = set([np.get('name-part-type') for (np, v) in name_cand])
        if len(types) != len(name_cand):
            continue

        names = [(np, v.text) for (np, v) in name_cand]
        # print [(np.get('name-part-type'), v) for (np, v) in names]
        record['other_names'].append(get_name_data(names))


def parse_identity(record, ident, places):
    main = ident.get('main') == 'true'
    # print ident, record['type'], record['program']

    for name in ident.findall('./name'):
        parse_name(name, record, main)

    for dob in ident.findall('./day-month-year'):
        if main or 'date_of_birth' not in record:
            record['date_of_birth'] = parse_date(dob)

    for pob in ident.findall('./place-of-birth'):
        addr = parse_addr(pob, places)
        parts = [addr.get('text'), addr.get('address1'), addr.get('address2'),
                 addr.get('city'), addr.get('region')]
        parts = [p for p in parts if p is not None]
        if main or 'place_of_birth' not in record:
            if len(parts):
                record['place_of_birth'] = ' / '.join(parts)
        if main or 'country_of_birth' not in record:
            if addr.get('country'):
                record['country_of_birth'] = addr.get('country')

    for addr in ident.findall('./address'):
        record['addresses'].append(parse_addr(addr, places))

    for doc in ident.findall('./identification-document'):
        country = doc.find('./issuer')
        if country is not None:
            if country.get('code'):
                country = normalize_country(country.get('code'))
            else:
                country = normalize_country(country.text)

        record['identities'].append({
            'country': country,
            'type': doc.get('document-type'),
            'number': doc.findtext('./number'),
        })


def parse_entry(emit, target, sanctions, places):
    node = target.find('./individual')
    if node is None:
        node = target.find('./entity')
    record = {
        'uid': make_id('ch', 'seco', target.get('ssid')),
        'type': node.tag,
        'program': sanctions.get(target.get('sanctions-set-id')),
        'function': node.findtext('./other-information'),
        'summary': node.findtext('./justification'),
        'other_names': [],
        'addresses': [],
        'identities': []
    }
    record.update(PUBLISHER)
    for ident in node.findall('./identity'):
        parse_identity(record, ident, places)

    # from pprint import pprint
    # pprint(record)
    emit.entity(record)


def seco_parse(emit, xmlfile):
    doc = etree.parse(xmlfile)

    sanctions = {}
    for sanc in doc.findall('.//sanctions-program'):
        key = sanc.find('./sanctions-set').get('ssid')
        label = sanc.findtext('./program-name[@lang="eng"]')
        sanctions[key] = label

    places = {}
    for place in doc.findall('.//place'):
        places[place.get('ssid')] = place

    for target in doc.findall('.//target'):
        if target.getparent().tag == 'modification':
            continue
        # print target.get('ssid')
        parse_entry(emit, target, sanctions, places)
