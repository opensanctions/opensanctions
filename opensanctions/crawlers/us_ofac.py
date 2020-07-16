# cf.
# https://github.com/archerimpact/SanctionsExplorer/blob/master/data/sdn_parser.py
# https://www.treasury.gov/resource-center/sanctions/SDN-List/Documents/sdn_advanced_notes.pdf
from pprint import pprint  # noqa
from followthemoney import model
from followthemoney.exc import InvalidData

# from followthemoney.types import registry
from os.path import commonprefix
from ftmstore.memorious import EntityEmitter

from opensanctions.util import jointext

CACHE = {}
REFERENCES = {}

TAG = "{http://www.un.org/sanctions/1.0}"

TYPES = {
    "Entity": "LegalEntity",
    "Individual": "Person",
    "Vessel": "Vessel",
    "Aircraft": "Airplane",
}

NAMES = {
    "Last Name": "lastName",
    "First Name": "firstName",
    "Middle Name": "middleName",
    "Maiden Name": "lastName",
    "Aircraft Name": "registrationNumber",
    "Entity Name": "name",
    "Vessel Name": "name",
    "Nickname": "weakAlias",
    "Patronymic": "fatherName",
    "Matronymic": "motherName",
}

FEATURES = {
    # Location
    "25": (None, None),
    # Title
    "26": (None, "position"),
    # Birthdate
    "8": ("Person", "birthDate"),
    # Place of Birth
    "9": ("Person", "birthPlace"),
    # Additional Sanctions Information -
    "125": (None, "notes"),
    # Gender
    "224": ("Person", "gender"),
    # Vessel Call Sign
    "1": ("Vessel", "callSign"),
    # Vessel Flag
    "3": ("Vessel", "flag"),
    # Vessel Owner
    "4": ("Vehicle", "owner"),
    # Vessel Tonnage
    "5": ("Vessel", "tonnage"),
    # Vessel Gross Registered Tonnage
    "6": ("Vessel", "grossRegisteredTonnage"),
    # VESSEL TYPE
    "2": ("Vehicle", "type"),
    # Nationality Country
    "10": ("Person", "nationality"),
    # Citizenship Country
    "11": ("Person", "nationality"),
    # Secondary sanctions risk:
    "504": (None, "program"),
    # Transactions Prohibited For Persons Owned or Controlled By U.S. Financial Institutions:
    "626": (None, "program"),
    # Website
    "14": (None, "website"),
    # Email Address
    "21": (None, "email"),
    # IFCA Determination -
    "104": (None, "classification"),
    # SWIFT/BIC
    "13": (None, "swiftBic"),
    # Phone Number
    "524": (None, "phone"),
    # Former Vessel Flag
    "24": (None, "pastFlags"),
    # Aircraft Construction Number (also called L/N or S/N or F/N)
    "44": (None, None),
    # Aircraft Manufacturer’s Serial Number (MSN)
    "50": ("Airplane", "serialNumber"),
    # Aircraft Manufacture Date
    "45": ("Vehicle", "buildDate"),
    # Aircraft Model
    "47": ("Vehicle", "model"),
    # Aircraft Operator
    "48": ("Vehicle", "operator"),
    # BIK (RU)
    "164": ("Company", "bikCode"),
    # UN/LOCODE
    "264": (None, None),
    # Aircraft Tail Number
    "64": ("Vehicle", "registrationNumber"),
    # Previous Aircraft Tail Number
    "49": (None, "icaoCode"),
    # Executive Order 13662 Directive Determination -
    "204": (None, "program"),
    # MICEX Code
    "304": (None, None),
    # Nationality of Registration
    "365": ("Thing", "country"),
    # D-U-N-S Number
    "364": ("LegalEntity", "dunsCode"),
    # Other Vessel Call Sign
    "425": (None, "callSign"),
    # Other Vessel Flag
    "424": (None, "flag"),
    # CAATSA Section 235 Information:
    "525": (None, "program"),
    # Other Vessel Type
    "526": ("Vehicle", "type"),
    # TODO: should we model these as BankAccount??
    # Digital Currency Address - XBT
    "344": (None, None),
    # Digital Currency Address - LTC
    "566": (None, None),
    # Aircraft Mode S Transponder Code
    "46": (None, "registrationNumber"),
    # Executive Order 13846 information:
    "586": (None, "program"),
    # Organization Established Date
    "646": ("Organization", "incorporationDate"),
    # Organization Type:
    "647": ("Organization", "legalForm"),
}

ADJACENT_FEATURES = [
    # Vessel Owner
    "4",
    # Aircraft Operator
    "48",
]

REGISTRATIONS = {
    "Cedula No.": ("LegalEntity", "idNumber"),
    "Passport": ("Person", "passportNumber"),
    "SSN": ("Person", "idNumber"),
    "R.F.C.": ("LegalEntity", "taxNumber"),
    "D.N.I.": ("LegalEntity", "idNumber"),
    "NIT #": ("LegalEntity", "idNumber"),
    "US FEIN": ("LegalEntity", ""),
    "Driver's License No.": ("Person", "idNumber"),
    "RUC #": ("LegalEntity", "taxNumber"),
    "N.I.E.": ("LegalEntity", "idNumber"),
    "C.I.F.": ("LegalEntity", "taxNumber"),
    "Business Registration Document #": ("LegalEntity", ""),
    "RIF #": ("LegalEntity", ""),
    "National ID No.": ("LegalEntity", "idNumber"),
    "Registration ID": ("LegalEntity", "registrationNumber"),
    "LE Number": ("LegalEntity", "registrationNumber"),
    "Bosnian Personal ID No.": ("Person", "idNumber"),
    "Registered Charity No.": ("Organization", "registrationNumber"),
    "V.A.T. Number": ("LegalEntity", "vatCode"),
    "Credencial electoral": ("LegalEntity", ""),
    "Kenyan ID No.": ("LegalEntity", "idNumber"),
    "Italian Fiscal Code": ("LegalEntity", "taxNumber"),
    "Serial No.": ("LegalEntity", ""),
    "C.U.I.T.": ("LegalEntity", "taxNumber"),
    "Tax ID No.": ("LegalEntity", "taxNumber"),
    "Moroccan Personal ID No.": ("LegalEntity", "idNumber"),
    "Public Security and Immigration No.": ("LegalEntity", ""),
    "C.U.R.P.": ("LegalEntity", ""),
    "British National Overseas Passport": ("Person", "passportNumber"),
    "C.R. No.": ("LegalEntity", ""),
    "UK Company Number": ("Person", "registrationNumber"),
    "Immigration No.": ("LegalEntity", ""),
    "Travel Document Number": ("Person", "passportNumber"),
    "Electoral Registry No.": ("LegalEntity", ""),
    "Identification Number": ("LegalEntity", "idNumber"),
    "Paraguayan tax identification number": ("LegalEntity", "taxNumber"),
    "National Foreign ID Number": ("LegalEntity", "idNumber"),
    "RFC": ("LegalEntity", "taxNumber"),
    "Diplomatic Passport": ("Person", "passportNumber"),
    "Dubai Chamber of Commerce Membership No.": ("LegalEntity", ""),
    "Trade License No.": ("LegalEntity", ""),
    "Commercial Registry Number": ("LegalEntity", "registrationNumber"),
    "Certificate of Incorporation Number": ("LegalEntity", "registrationNumber"),
    "Cartilla de Servicio Militar Nacional": ("LegalEntity", ""),
    "C.U.I.P.": ("LegalEntity", ""),
    "Vessel Registration Identification": ("Vessel", "imoNumber"),
    "Personal ID Card": ("LegalEntity", "idNumber"),
    "VisaNumberID": ("LegalEntity", ""),
    "Matricula Mercantil No": ("LegalEntity", ""),
    "Residency Number": ("Person", ""),
    "Numero Unico de Identificacao Tributaria (NUIT)": ("LegalEntity", ""),
    "CNP (Personal Numerical Code)": ("LegalEntity", ""),
    "Romanian Permanent Resident": ("LegalEntity", "idNumber"),
    "Government Gazette Number": ("LegalEntity", ""),
    "Fiscal Code": ("LegalEntity", "taxNumber"),
    "Pilot License Number": ("LegalEntity", ""),
    "Romanian C.R.": ("LegalEntity", ""),
    "Folio Mercantil No.": ("LegalEntity", ""),
    "Istanbul Chamber of Comm. No.": ("LegalEntity", "registrationNumber"),
    "Turkish Identification Number": ("LegalEntity", "idNumber"),
    "Romanian Tax Registration": ("LegalEntity", "taxNumber"),
    "Stateless Person Passport": ("Person", "passportNumber"),
    "Stateless Person ID Card": ("Person", "idNumber"),
    "Refugee ID Card": ("Person", "idNumber"),
    "Afghan Money Service Provider License Number": ("LegalEntity", ""),
    "MMSI": ("Thing", "mmsi"),
    "Company Number": ("LegalEntity", "registrationNumber"),
    "Public Registration Number": ("LegalEntity", "registrationNumber"),
    "RTN": ("LegalEntity", ""),
    "Numero de Identidad": ("LegalEntity", "idNumber"),
    "SRE Permit No.": ("LegalEntity", ""),
    "Tazkira National ID Card": ("LegalEntity", "idNumber"),
    "License": ("LegalEntity", ""),
    "Chinese Commercial Code": ("LegalEntity", ""),
    "I.F.E.": ("LegalEntity", ""),
    "Branch Unit Number": ("LegalEntity", ""),
    "Enterprise Number": ("LegalEntity", "registrationNumber"),
    "Citizen's Card Number": ("LegalEntity", "idNumber"),
    "UAE Identification": ("LegalEntity", ""),
    "United Social Credit Code Certificate (USCCC)": ("LegalEntity", ""),
    "Tarjeta Profesional": ("LegalEntity", "idNumber"),
    "Chamber of Commerce Number": ("LegalEntity", "registrationNumber"),
    "Legal Entity Number": ("LegalEntity", "registrationNumber"),
    "Business Number": ("LegalEntity", "registrationNumber"),
    "Birth Certificate Number": ("LegalEntity", ""),
    "Business Registration Number": ("LegalEntity", "registrationNumber"),
    "Registration Number": ("LegalEntity", "registrationNumber"),
    "Aircraft Serial Identification": ("Airplane", "serialNumber"),
}

RELATIONS = {
    "15003": ("Ownership", "asset", "owner", "role"),
    "15002": ("Representation", "agent", "client", "role"),
    # Providing support to:
    "15001": ("UnknownLink", "subject", "object", "role"),
    # Leader or official of
    "91725": ("Directorship", "director", "organization", "role"),
    # Principal Executive Officer
    "91900": ("Directorship", "director", "organization", "role"),
    # Associate Of
    "1555": ("UnknownLink", "subject", "object", "role"),
    # playing a significant role in
    "91422": ("Membership", "member", "organization", "role"),
    # Family member of
    "15004": ("Family", "person", "relative", "relationship"),
}


def remove_namespace(doc):
    """Remove namespace in the passed document in place."""
    for elem in doc.getiterator():
        if elem.tag.startswith(TAG):
            elem.tag = elem.tag[len(TAG) :]
    return doc


def qpath(name):
    return "./%s" % name


def load_ref_values(doc):
    ref_value_sets = doc.find(".//ReferenceValueSets")
    for ref_set in ref_value_sets.getchildren():
        for ref_val in ref_set.getchildren():
            data = dict(ref_val.attrib)
            assert "Value" not in data
            data["Value"] = ref_val.text
            REFERENCES[(ref_val.tag, data.get("ID"))] = data


def ref_get(type_, id_):
    return REFERENCES[(type_, id_)]


def ref_value(type_, id_):
    return ref_get(type_, id_).get("Value")


def deref(doc, tag, value, attr=None, key="ID", element=False):
    cache = (tag, value, attr, key, element)
    if cache in CACHE:
        return CACHE[cache]
    query = '//%s[@%s="%s"]' % (tag, key, value)
    for node in doc.findall(query):
        if element:
            return node
        if attr is not None:
            value = node.get(attr)
        else:
            value = node.text
        CACHE[cache] = value
        return value


def add_schema(entity, addition):
    try:
        entity.schema = model.common_schema(entity.schema, addition)
    except InvalidData:
        for schema in model.schemata.values():
            if schema.is_a(entity.schema) and schema.is_a(addition):
                entity.schema = schema
                return
        raise


def disjoint_schema(entity, addition):
    for schema in model.schemata.values():
        if schema.is_a(entity.schema) and schema.is_a(addition):
            return False
    return True


def parse_date_single(node):
    parts = (
        node.findtext("./Year"),
        node.findtext("./Month"),
        node.findtext("./Day"),
    )
    return "-".join(parts)


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
    start = date.find("./Start")
    start_from = parse_date_single(start.find("./From"))
    start_to = parse_date_single(start.find("./To"))
    end = date.find("./End")
    end_from = parse_date_single(end.find("./From"))
    end_to = parse_date_single(end.find("./To"))
    return date_common_prefix(start_from, start_to, end_from, end_to)


def parse_location(entity, doc, location_id):
    location = doc.find("./Locations/Location[@ID='%s']" % location_id)
    parts = {}
    for part in location.findall("./LocationPart"):
        type_ = ref_value("LocPartType", part.get("LocPartTypeID"))
        parts[type_] = part.findtext("./LocationPartValue/Value")
    address = jointext(
        parts.get("Unknown"),
        parts.get("ADDRESS1"),
        parts.get("ADDRESS2"),
        parts.get("ADDRESS2"),
        parts.get("CITY"),
        parts.get("POSTAL CODE"),
        parts.get("REGION"),
        parts.get("STATE/PROVINCE"),
        sep=", ",
    )
    entity.add("address", address)

    for area in location.findall("./LocationAreaCode"):
        area_code = ref_get("AreaCode", area.get("AreaCodeID"))
        country = ref_get("Country", area_code.get("CountryID"))
        entity.add("country", country.get("ISO2"))
    for country in location.findall("./LocationCountry"):
        country = ref_get("Country", country.get("CountryID"))
        entity.add("country", country.get("ISO2"))

    if not entity.has("country"):
        entity.add("country", parts.get("Unknown"))


def parse_alias(party, parts, alias):
    primary = alias.get("Primary") == "true"
    weak = alias.get("LowQuality") == "true"
    alias_type = ref_value("AliasType", alias.get("AliasTypeID"))
    for name in alias.findall("./DocumentedName"):
        data = {}
        for name_part in name.findall("./DocumentedNamePart"):
            value = name_part.find("./NamePartValue")
            type_ = parts.get(value.get("NamePartGroupID"))
            field = NAMES[type_]
            data[field] = value.text
            if field != "name" and not weak:
                party.add(field, value.text)
            # print(field, value.text)
        name = jointext(
            data.get("firstName"),
            data.get("middleName"),
            data.get("fatherName"),
            data.get("lastName"),
            data.get("name"),
        )
        if primary:
            party.add("name", name)
        elif alias_type == "F.K.A.":
            party.add("previousName", name)
        else:
            party.add("alias", name)


def make_adjacent(emitter, name):
    entity = emitter.make("LegalEntity")
    entity.make_id("Named", name)
    entity.add("name", name)
    emitter.emit(entity)
    return entity


def parse_party(emitter, doc, distinct_party):
    profile = distinct_party.find("Profile")
    sub_type = ref_get("PartySubType", profile.get("PartySubTypeID"))
    schema = TYPES.get(sub_type.get("Value"))
    type_ = ref_value("PartyType", sub_type.get("PartyTypeID"))
    schema = TYPES.get(type_, schema)
    if schema is None:
        emitter.log.error("Unknown party type: %s", type_)
        return
    party = emitter.make(schema)
    party.id = "ofac-%s" % profile.get("ID")
    party.add("notes", distinct_party.findtext("Comment"))

    for identity in profile.findall("./Identity"):
        parts = {}
        for group in identity.findall(".//NamePartGroup"):
            type_id = group.get("NamePartTypeID")
            parts[group.get("ID")] = ref_value("NamePartType", type_id)

        for alias in identity.findall("./Alias"):
            parse_alias(party, parts, alias)

    #     identity_id = identity.get("ID")
    #     query = '//%s[@IdentityID="%s"]' % (qtag("IDRegDocument"), identity_id)
    #     for idreg in doc.findall(query):
    #         authority = idreg.findtext(qpath("IssuingAuthority"))
    #         number = idreg.findtext(qpath("IDRegistrationNo"))
    #         type_ = deref(doc, "IDRegDocType", idreg.get("IDRegDocTypeID"))
    #         if authority == "INN":
    #             party.add("innCode", number)
    #             continue
    #         if authority == "OGRN":
    #             party.schema = model.get("Company")
    #             party.add("ogrnCode", number)
    #             continue
    #         if type_ in REGISTRATIONS.keys():
    #             schema, attr = REGISTRATIONS.get(type_)
    #         else:
    #             emitter.log.error("Unknown type: %s", type_)
    #             continue
    #         if attr == "imoNumber" and party.schema.is_a("LegalEntity"):
    #             # https://en.wikipedia.org/wiki/IMO_number
    #             # vessel owning Companies can have imoNumber too
    #             party.add("idNumber", number)
    #             # party.schema = model.get("Company")
    #         else:
    #                add_schema(party, schema)
    #             if attr:
    #                 party.add(attr, number)

    for feature in profile.findall("./Feature"):
        feature_id = feature.get("FeatureTypeID")
        # feature_type = ref_value("FeatureType", feature_id)
        # if feature_id not in FEATURES:
        #     print("    # %s" % feature_type)
        #     print("    '%s': (None, None)," % feature_id)
        schema, prop = FEATURES[feature_id]
        if schema is not None:
            add_schema(party, schema)
        if prop is None:
            continue

        period = feature.find(".//DatePeriod")
        if period is not None:
            party.add(prop, parse_date_period(period))

        # vlocation = feature.find(".//VersionLocation")
        # if vlocation is not None:
        #     parse_location(party, doc, vlocation.get("LocationID"))

        detail = feature.find(".//VersionDetail")
        if detail is not None:
            reference_id = detail.get("DetailReferenceID")
            if reference_id is not None:
                value = ref_value("DetailReference", reference_id)
            else:
                value = detail.text
            if feature_id in ADJACENT_FEATURES:
                value = make_adjacent(emitter, value)
            party.add(prop, value)

    emitter.emit(party)
    # pprint(party.to_dict())
    emitter.log.info("[%s] %s", party.schema.name, party.caption)


def parse_entry(emitter, doc, entry):
    party = emitter.make("Thing")
    party.id = "ofac-%s" % entry.get("ProfileID")

    sanction = emitter.make("Sanction")
    sanction.make_id("Sanction", party.id, entry.get("ID"))
    sanction.add("entity", party)
    sanction.add("authority", "US Office of Foreign Asset Control")
    sanction.add("program", ref_value("List", entry.get("ListID")))

    for event in entry.findall("./EntryEvent"):
        sanction.add("startDate", parse_date_single(event.find("./Date")))
        sanction.add("summary", event.findtext("./Comment"))
        basis = ref_value("LegalBasis", event.get("LegalBasisID"))
        sanction.add("reason", basis)

    for measure in entry.findall("./SanctionsMeasure"):
        sanction.add("summary", measure.findtext("./Comment"))
        type_id = measure.get("SanctionsTypeID")
        sanction.add("program", ref_value("SanctionsType", type_id))

    emitter.emit(sanction)
    # pprint(sanction.to_dict())


def parse_relation(emitter, doc, relation):
    type_id = relation.get("RelationTypeID")
    type_ = ref_value("RelationType", relation.get("RelationTypeID"))
    # if type_id not in RELATIONS:
    #     from_party = emitter.dataset.get(from_party.id)
    #     to_party = emitter.dataset.get(to_party.id)
    #     print(from_party, ">>", type_, ">>", to_party, " :: ", type_id)
    #     return
    schema, from_attr, to_attr, desc_attr = RELATIONS[type_id]
    entity = emitter.make(schema)
    from_id = "ofac-%s" % relation.get("From-ProfileID")
    from_party = emitter.dataset.get(from_id)
    from_range = entity.schema.get(from_attr).range
    to_id = "ofac-%s" % relation.get("To-ProfileID")
    to_party = emitter.dataset.get(to_id)
    to_range = entity.schema.get(to_attr).range

    # HACK: Looks like OFAC just has some link in a direction that makes no
    # semantic validity, so we're flipping them here.
    if disjoint_schema(from_party, from_range) or disjoint_schema(to_party, to_range):
        from_party, to_party = to_party, from_party

    add_schema(from_party, from_range)
    add_schema(to_party, to_range)
    emitter.emit(from_party)
    emitter.emit(to_party)
    entity.make_id("Relation", from_party.id, to_party.id, relation.get("ID"))
    entity.add(from_attr, from_party)
    entity.add(to_attr, to_party)
    entity.add(desc_attr, type_)
    entity.add("summary", relation.findtext("./Comment"))
    emitter.emit(entity)
    emitter.log.info("Relation [%s]-[%s]->[%s]", from_party, type_, to_party)
    # pprint(entity.to_dict())


def parse(context, data):
    emitter = EntityEmitter(context)
    with context.http.rehash(data) as res:
        doc = remove_namespace(res.xml)
        load_ref_values(doc)

        for distinct_party in doc.findall(".//DistinctParty"):
            parse_party(emitter, doc, distinct_party)

        for entry in doc.findall(".//SanctionsEntry"):
            parse_entry(emitter, doc, entry)

        for relation in doc.findall(".//ProfileRelationship"):
            parse_relation(emitter, doc, relation)

    emitter.finalize()
