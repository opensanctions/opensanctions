# cf.
# https://github.com/archerimpact/SanctionsExplorer/blob/master/data/sdn_parser.py
# https://www.treasury.gov/resource-center/sanctions/SDN-List/Documents/sdn_advanced_notes.pdf
from pprint import pprint  # noqa
from followthemoney import model
from followthemoney.types import registry
from os.path import commonprefix

from opensanctions.util import EntityEmitter
from opensanctions.util import jointext

CACHE = {}

TYPES = {
    'Entity': 'LegalEntity',
    'Individual': 'Person',
    'Vessel': 'Vessel',
    'Aircraft': 'Airplane',
}

NAMES = {
    'Last Name': 'lastName',
    'First Name': 'firstName',
    'Middle Name': 'middleName',
    'Maiden Name': 'lastName',
    'Aircraft Name': 'name',
    'Entity Name': 'name',
    'Vessel Name': 'name',
    'Nickname': 'weakAlias',
    'Patronymic': 'fatherName',
    'Matronymic': 'motherName',
}


FEATURES = {
    "Vessel Call Sign": ('callSign', 'Vessel'),
    "VESSEL TYPE": ('type', 'Vessel'),
    "Vessel Flag": ('flag', 'Vessel'),
    "Vessel Owner": ('owner', 'Vessel'),
    "Vessel Tonnage": ('tonnage', 'Vessel'),
    "Vessel Gross Registered Tonnage": ('tonnage', 'Vessel'),  # noqa
    "Birthdate": ('birthDate', 'Person'),
    "Place of Birth": ('birthPlace', 'Person'),
    "Nationality Country": ('nationality', 'LegalEntity'),
    "Citizenship Country": ('nationality', 'Person'),
    "SWIFT/BIC": ('swiftBic', 'Company'),
    "Website": ('website', 'LegalEntity'),
    "Email Address": ('email', 'LegalEntity'),
    "Former Vessel Flag": ('pastFlags', 'Vessel'),
    "Location": ('address', 'LegalEntity'),
    "Title": ('title', 'LegalEntity'),
    "Aircraft Construction Number (also called L/N or S/N or F/N)": ('serialNumber', 'Airplane'),  # noqa
    "Aircraft Manufacture Date": ('buildDate', 'Airplane'),
    "Aircraft Model": ('model', 'Airplane'),
    "Aircraft Operator": ('operator', 'Airplane'),
    "Previous Aircraft Tail Number": ('registrationNumber', 'Airplane'),  # noqa
    "Aircraft Manufacturer’s Serial Number (MSN)": ('serialNumber', 'Airplane'),  # noqa
    "Aircraft Tail Number": ('registrationNumber', 'Airplane'),  # noqa
    "IFCA Determination -": ('notes', 'LegalEntity'),
    "Additional Sanctions Information -": ('notes', 'LegalEntity'),  # noqa
    "BIK (RU)": ('bikCode', 'Company'),
    "Executive Order 13662 Directive Determination -": ('notes', 'LegalEntity'),  # noqa
    "Gender": ('gender', 'Person'),
    "UN/LOCODE": (None, 'LegalEntity'),
    "MICEX Code": (None, 'Company'),
    "Digital Currency Address - XBT": (None, 'LegalEntity'),
    "D-U-N-S Number": ('dunsCode', 'LegalEntity'),
    "Nationality of Registration": ('country', 'LegalEntity'),
    "Other Vessel Flag": ('pastFlags', 'Vessel'),
    "Other Vessel Call Sign": ('callSign', 'Vessel'),
    "Secondary sanctions risk:": ('notes', 'LegalEntity'),
    "Phone Number": ('phone', 'LegalEntity'),
    "CAATSA Section 235 Information:": ('notes', 'LegalEntity'),
    "Other Vessel Type": ('pastTypes', 'LegalEntity'),
}

REGISTRATIONS = {
    "Cedula No.": ('LegalEntity', 'idNumber'),
    "Passport": ('Person', 'passportNumber'),
    "SSN": ('Person', 'idNumber'),
    "R.F.C.": ('LegalEntity', 'taxNumber'),
    "D.N.I.": ('LegalEntity', 'idNumber'),
    "NIT #": ('LegalEntity', 'idNumber'),
    "US FEIN": ('LegalEntity', ''),
    "Driver's License No.": ('Person', 'idNumber'),
    "RUC #": ('LegalEntity', 'taxNumber'),
    "N.I.E.": ('LegalEntity', 'idNumber'),
    "C.I.F.": ('LegalEntity', 'taxNumber'),
    "Business Registration Document #": ('LegalEntity', ''),
    "RIF #": ('LegalEntity', ''),
    "National ID No.": ('LegalEntity', 'idNumber'),
    "Registration ID": ('LegalEntity', 'registrationNumber'),
    "LE Number": ('LegalEntity', 'registrationNumber'),
    "Bosnian Personal ID No.": ('Person', 'idNumber'),
    "Registered Charity No.": ('Organization', 'registrationNumber'),
    "V.A.T. Number": ('LegalEntity', 'vatCode'),
    "Credencial electoral": ('LegalEntity', ''),
    "Kenyan ID No.": ('LegalEntity', 'idNumber'),
    "Italian Fiscal Code": ('LegalEntity', 'taxNumber'),
    "Serial No.": ('LegalEntity', ''),
    "C.U.I.T.": ('LegalEntity', 'taxNumber'),
    "Tax ID No.": ('LegalEntity', 'taxNumber'),
    "Moroccan Personal ID No.": ('LegalEntity', 'idNumber'),
    "Public Security and Immigration No.": ('LegalEntity', ''),
    "C.U.R.P.": ('LegalEntity', ''),
    "British National Overseas Passport": ('Person', 'passportNumber'),
    "C.R. No.": ('LegalEntity', ''),
    "UK Company Number": ('Person', 'registrationNumber'),
    "Immigration No.": ('LegalEntity', ''),
    "Travel Document Number": ('Person', 'passportNumber'),
    "Electoral Registry No.": ('LegalEntity', ''),
    "Identification Number": ('LegalEntity', 'idNumber'),
    "Paraguayan tax identification number": ('LegalEntity', 'taxNumber'),
    "National Foreign ID Number": ('LegalEntity', 'idNumber'),
    "RFC": ('LegalEntity', 'taxNumber'),
    "Diplomatic Passport": ('Person', 'passportNumber'),
    "Dubai Chamber of Commerce Membership No.": ('LegalEntity', ''),
    "Trade License No.": ('LegalEntity', ''),
    "Commercial Registry Number": ('LegalEntity', 'registrationNumber'),
    "Certificate of Incorporation Number": ('LegalEntity', 'registrationNumber'),  # noqa
    "Cartilla de Servicio Militar Nacional": ('LegalEntity', ''),
    "C.U.I.P.": ('LegalEntity', ''),
    "Vessel Registration Identification": ('Vessel', ''),
    "Personal ID Card": ('LegalEntity', 'idNumber'),
    "VisaNumberID": ('LegalEntity', ''),
    "Matricula Mercantil No": ('LegalEntity', ''),
    "Residency Number": ('Person', ''),
    "Numero Unico de Identificacao Tributaria (NUIT)": ('LegalEntity', ''),
    "CNP (Personal Numerical Code)": ('LegalEntity', ''),
    "Romanian Permanent Resident": ('LegalEntity', 'idNumber'),
    "Government Gazette Number": ('LegalEntity', ''),
    "Fiscal Code": ('LegalEntity', 'taxNumber'),
    "Pilot License Number": ('LegalEntity', ''),
    "Romanian C.R.": ('LegalEntity', ''),
    "Folio Mercantil No.": ('LegalEntity', ''),
    "Istanbul Chamber of Comm. No.": ('LegalEntity', 'registrationNumber'),
    "Turkish Identificiation Number": ('LegalEntity', 'idNumber'),
    "Romanian Tax Registration": ('LegalEntity', 'taxNumber'),
    "Stateless Person Passport": ('Person', 'passportNumber'),
    "Stateless Person ID Card": ('Person', 'idNumber'),
    "Refugee ID Card": ('Person', 'idNumber'),
    "Afghan Money Service Provider License Number": ('LegalEntity', ''),
    "MMSI": ('Vessel', 'mmsi'),
    "Company Number": ('LegalEntity', 'registrationNumber'),
    "Public Registration Number": ('LegalEntity', 'registrationNumber'),
    "RTN": ('LegalEntity', ''),
    "Numero de Identidad": ('LegalEntity', 'idNumber'),
    "SRE Permit No.": ('LegalEntity', ''),
    "Tazkira National ID Card": ('LegalEntity', 'idNumber'),
    "License": ('LegalEntity', ''),
    "Chinese Commercial Code": ('LegalEntity', ''),
    "I.F.E.": ('LegalEntity', ''),
    "Branch Unit Number": ('LegalEntity', ''),
    "Enterprise Number": ('LegalEntity', 'registrationNumber'),
    "Citizen's Card Number": ('LegalEntity', 'idNumber'),
    "UAE Identification": ('LegalEntity', ''),
    "United Social Credit Code Certificate (USCCC)": ('LegalEntity', ''),
    "Tarjeta Profesional": ('LegalEntity', 'idNumber'),
    "Chamber of Commerce Number": ('LegalEntity', 'registrationNumber'),
    "Legal Entity Number": ('LegalEntity', 'registrationNumber'),
    "Business Number": ('LegalEntity', 'registrationNumber'),
    "Birth Certificate Number": ('LegalEntity', ''),
    "Business Registration Number": ('LegalEntity', 'registrationNumber'),
    "Registration Number": ('LegalEntity', 'registrationNumber'),
}

RELATIONS = {
    'Associate Of': ('Associate', 'person', 'associate'),
    'Providing support to': ('UnknownLink', 'subject', 'object'),
    'Acting for or on behalf of': ('Representation', 'agent', 'client'),
    'Owned or Controlled By': ('Ownership', 'asset', 'owner'),
    'Family member of': ('Family', 'person', 'relative'),
    'playing a significant role in': ('Membership', 'member', 'organization'),
    'Leader or official of': ('Directorship', 'director', 'organization'),
}


def qtag(name):
    return '{http://www.un.org/sanctions/1.0}%s' % name


def qpath(name):
    return './/%s' % qtag(name)


def deref(doc, tag, value, attr=None, key='ID', element=False):
    cache = (tag, value, attr, key, element)
    if cache in CACHE:
        return CACHE[cache]
    query = '//%s[@%s="%s"]' % (qtag(tag), key, value)
    for node in doc.findall(query):
        if element:
            return node
        if attr is not None:
            value = node.get(attr)
        else:
            value = node.text
        CACHE[cache] = value
        return value


def parse_date_single(node):
    return '-'.join((node.findtext(qpath('Year')),
                     node.findtext(qpath('Month')).zfill(2),
                     node.findtext(qpath('Day')).zfill(2)))


def date_common_prefix(*dates):
    prefix = commonprefix(dates)[:10]
    if len(prefix) < 10:
        prefix = prefix[:7]
    if len(prefix) < 7:
        prefix = prefix[:4]
    if len(prefix) < 4:
        prefix = None
    return prefix


def parse_date_period(date):
    start = date.find(qpath('Start'))
    start_from = parse_date_single(start.find(qpath('From')))
    start_to = parse_date_single(start.find(qpath('To')))
    end = date.find(qpath('End'))
    end_from = parse_date_single(end.find(qpath('From')))
    end_to = parse_date_single(end.find(qpath('To')))
    return date_common_prefix(start_from, start_to, end_from, end_to)


def parse_feature(doc, feature):
    detail = feature.find(qpath('VersionDetail'))

    period = feature.find(qpath('DatePeriod'))
    if period is not None:
        return parse_date_period(period)

    vlocation = feature.find(qpath('VersionLocation'))
    if vlocation is not None:
        location = deref(doc, 'Location',
                         vlocation.get('LocationID'),
                         element=True)
        country_code = None
        parts = {}
        for part in location.findall(qpath('LocationPart')):
            type_id = part.get('LocPartTypeID')
            type_ = deref(doc, 'LocPartType', type_id)
            value = part.findtext(qpath('Value'))
            parts[type_] = value
        address = jointext(parts.get('Unknown'),
                           parts.get('ADDRESS1'),
                           parts.get('ADDRESS2'),
                           parts.get('ADDRESS2'),
                           parts.get('CITY'),
                           parts.get('POSTAL CODE'),
                           parts.get('REGION'),
                           parts.get('STATE/PROVINCE'),
                           sep=', ')

        for area in location.findall(qpath('LocationAreaCode')):
            country_id = deref(doc, 'AreaCode', area.get('AreaCodeID'), 'CountryID')  # noqa
            country_code = deref(doc, 'Country', country_id, 'ISO2')
        for country in location.findall(qpath('LocationCountry')):
            country_id = country.get('CountryID')
            country_code = deref(doc, 'Country', country_id, 'ISO2')
        return (address, country_code)

    if detail is not None:
        reference_id = detail.get('DetailReferenceID')
        if reference_id is not None:
            return deref(doc, 'DetailReference', reference_id)
        return detail.text


def parse_alias(party, doc, alias):
    primary = alias.get('Primary') == 'true'
    weak = alias.get('LowQuality') == 'true'
    alias_type = deref(doc, 'AliasType', alias.get('AliasTypeID'))
    data = {}
    for name_part in alias.findall(qpath('DocumentedNamePart')):
        value = name_part.find(qpath('NamePartValue'))
        type_id = value.get('NamePartGroupID')
        type_id = deref(doc, 'NamePartGroup', type_id, 'NamePartTypeID')  # noqa
        part_type = deref(doc, 'NamePartType', type_id)
        field = NAMES.get(part_type)
        data[field] = value.text
        if field != 'name' and not weak:
            party.add(field, value.text)
        # print(field, value.text)
    name = jointext(data.get('firstName'),
                    data.get('middleName'),
                    data.get('fatherName'),
                    data.get('lastName'),
                    data.get('name'))
    if primary:
        party.add('name', name)
    elif alias_type == 'F.K.A.':
        party.add('previousName', name)
    else:
        party.add('alias', name)


def parse_party(emitter, doc, distinct_party):
    profile = distinct_party.find(qpath('Profile'))
    sub_type_ = profile.get('PartySubTypeID')
    sub_type = deref(doc, 'PartySubType', sub_type_)
    type_ = deref(doc, 'PartySubType', sub_type_, 'PartyTypeID')
    type_ = deref(doc, 'PartyType', type_)
    schema = TYPES.get(type_, TYPES.get(sub_type))
    party = emitter.make(schema)
    party.make_id('Profile', profile.get('ID'))
    party.add('notes', distinct_party.findtext(qpath('Comment')))

    for identity in profile.findall(qpath('Identity')):
        for alias in identity.findall(qpath('Alias')):
            parse_alias(party, doc, alias)

        identity_id = identity.get('ID')
        query = '//%s[@IdentityID="%s"]' % (qtag('IDRegDocument'), identity_id)
        for idreg in doc.findall(query):
            authority = idreg.findtext(qpath('IssuingAuthority'))
            number = idreg.findtext(qpath('IDRegistrationNo'))
            type_ = deref(doc, 'IDRegDocType', idreg.get('IDRegDocTypeID'))
            if authority == 'INN':
                party.add('innCode', number)
                continue
            if authority == 'OGRN':
                party.schema = model.get('Company')
                party.add('ogrnCode', number)
                continue
            schema, attr = REGISTRATIONS.get(type_)
            party.schema = model.common_schema(party.schema, schema)
            if len(attr):
                party.add(attr, number)

    for feature in profile.findall(qpath('Feature')):
        feature_type = deref(doc, 'FeatureType', feature.get('FeatureTypeID'))
        attr, schema = FEATURES.get(feature_type)
        party.schema = model.common_schema(party.schema, schema)
        if len(attr):
            value = parse_feature(doc, feature)
            if isinstance(value, tuple):
                value, country_code = value
                if party.schema.get(attr).type == registry.country:
                    value = country_code
                else:
                    party.add('country', country_code)
            party.add(attr, value, quiet=True)

    emitter.emit(party)
    emitter.log.info("[%s] %s", party.schema.name, party.caption)


def parse_entry(emitter, doc, entry):
    party = emitter.make('LegalEntity')
    party.make_id('Profile', entry.get('ProfileID'))

    sanction = emitter.make('Sanction')
    sanction.make_id('Sanction', party.id, entry.get('ID'))
    sanction.add('entity', party)
    sanction.add('authority', 'US Office of Foreign Asset Control')

    sanctions_list = deref(doc, 'List', entry.get('ListID'))
    sanction.add('program', sanctions_list)

    for event in entry.findall(qpath('EntryEvent')):
        date = parse_date_single(event.find(qpath('Date')))
        sanction.add('startDate', date)
        sanction.add('summary', event.findtext(qpath('Comment')))
        reason = deref(doc, 'LegalBasis', event.get('LegalBasisID'))
        sanction.add('reason', reason)

    for measure in entry.findall(qpath('SanctionsMeasure')):
        sanction.add('summary', measure.findtext(qpath('Comment')))
        type_ = deref(doc, 'SanctionsType', event.get('SanctionsTypeID'))
        sanction.add('program', type_)

    emitter.emit(sanction)
    # pprint(sanction.to_dict())


def parse_relation(emitter, doc, relation):
    from_party = emitter.make('LegalEntity')
    from_party.make_id('Profile', relation.get('From-ProfileID'))
    to_party = emitter.make('LegalEntity')
    to_party.make_id('Profile', relation.get('To-ProfileID'))

    type_ = deref(doc, 'RelationType', relation.get('RelationTypeID'))
    schema, from_attr, to_attr = RELATIONS.get(type_)
    entity = emitter.make(schema)
    entity.make_id('Relation', schema, relation.get('ID'))
    entity.add(from_attr, from_party)
    entity.add(to_attr, to_party)
    emitter.emit(entity)


def parse(context, data):
    emitter = EntityEmitter(context)
    with context.http.rehash(data) as res:
        doc = res.xml
        for distinct_party in doc.findall(qpath('DistinctParty')):
            parse_party(emitter, doc, distinct_party)

        for entry in doc.findall(qpath('SanctionsEntry')):
            parse_entry(emitter, doc, entry)

        for relation in doc.findall(qpath('ProfileRelationship')):
            parse_relation(emitter, doc, relation)

    emitter.finalize()
