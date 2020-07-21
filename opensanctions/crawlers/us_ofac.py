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

REG_ID = {
    # Passport
    "1571": (None, None),
    # US FEIN
    "1576": (None, None),
    # SSN
    "1572": (None, None),
    # Cedula No.
    "1570": ("LegalEntity", "idNumber"),
    # NIT #
    "1575": ("LegalEntity", "idNumber"),
    # UK Company Number
    "1603": ("LegalEntity", "registrationNumber"),
    # Serial No.
    "1593": (None, None),
    # Kenyan ID No.
    "1591": ("LegalEntity", "idNumber"),
    # R.F.C.
    "1573": (None, None),
    # Credencial electoral
    "1590": (None, None),
    # Driver's License No.
    "1577": (None, None),
    # D.N.I.
    "1574": (None, None),
    # Italian Fiscal Code
    "1592": ("LegalEntity", "taxNumber"),
    # V.A.T. Number
    "1589": ("LegalEntity", "vatCode"),
    # National ID No.
    "1584": ("LegalEntity", "idNumber"),
    # Public Security and Immigration No.
    "1598": (None, None),
    # Registration ID
    "1585": ("LegalEntity", "registrationNumber"),
    # Identification Number
    "1608": ("LegalEntity", "idNumber"),
    # Moroccan Personal ID No.
    "1597": ("LegalEntity", "idNumber"),
    # Registered Charity No.
    "1588": (None, None),
    # Bosnian Personal ID No.
    "1587": ("LegalEntity", "idNumber"),
    # C.U.I.T.
    "1595": (None, None),
    # LE Number
    "1586": (None, None),
    # Business Registration Document #
    "1581": ("LegalEntity", "registrationNumber"),
    # RUC #
    "1578": ("LegalEntity", "taxNumber"),
    # Tax ID No.
    "1596": ("LegalEntity", "taxNumber"),
    # C.U.R.P.
    "1600": (None, None),
    # British National Overseas Passport
    "1601": (None, None),
    # Immigration No.
    "1604": (None, None),
    # Travel Document Number
    "1605": (None, None),
    # C.R. No.
    "1602": (None, None),
    # Electoral Registry No.
    "1607": (None, None),
    # Paraguayan tax identification number
    "1609": ("LegalEntity", "taxNumber"),
    # National Foreign ID Number
    "1611": ("LegalEntity", "idNumber"),
    # RFC
    "1612": (None, None),
    # Diplomatic Passport
    "1613": (None, None),
    # Commercial Registry Number
    "1619": ("LegalEntity", "registrationNumber"),
    # Certificate of Incorporation Number
    "1620": ("LegalEntity", "registrationNumber"),
    # Personal ID Card
    "1627": ("LegalEntity", "idNumber"),
    # VisaNumberID
    "1630": (None, None),
    # Residency Number
    "1632": (None, None),
    # Matricula Mercantil No
    "1631": (None, None),
    # Registration Number
    "91761": ("LegalEntity", "registrationNumber"),
    # Numero Unico de Identificacao Tributaria (NUIT)
    "1633": (None, None),
    # N.I.E.
    "1579": (None, None),
    # C.I.F.
    "1580": (None, None),
    # C.U.I.P.
    "1625": (None, None),
    # Cartilla de Servicio Militar Nacional
    "1624": (None, None),
    # Folio Mercantil No.
    "1643": (None, None),
    # RIF #
    "1582": (None, None),
    # Istanbul Chamber of Comm. No.
    "1644": ("LegalEntity", "registrationNumber"),
    # Turkish Identification Number
    "1645": ("LegalEntity", "idNumber"),
    # Dubai Chamber of Commerce Membership No.
    "1614": (None, None),
    # Refugee ID Card
    "1649": (None, None),
    # Stateless Person ID Card
    "1648": (None, None),
    # Stateless Person Passport
    "1647": (None, None),
    # CNP (Personal Numerical Code)
    "1634": ("LegalEntity", "idNumber"),
    # Romanian Permanent Resident
    "1635": (None, None),
    # Romanian C.R.
    "1642": (None, None),
    # Romanian Tax Registration
    "1646": ("LegalEntity", "taxNumber"),
    # Fiscal Code
    "1638": (None, None),
    # Numero de Identidad
    "91482": ("LegalEntity", "idNumber"),
    # Afghan Money Service Provider License Number
    "91236": (None, None),
    # Vessel Registration Identification
    "1626": ("Vehicle", "registrationNumber"),
    # MMSI
    "91264": ("Vessel", "mmsi"),
    # Company Number
    "91412": ("LegalEntity", "registrationNumber"),
    # Public Registration Number
    "91475": ("LegalEntity", "registrationNumber"),
    # RTN
    "91481": (None, None),
    # SRE Permit No.
    "91484": (None, None),
    # Chinese Commercial Code
    "91508": ("LegalEntity", "registrationNumber"),
    # Tazkira National ID Card
    "91492": ("LegalEntity", "idNumber"),
    # Government Gazette Number
    "1636": (None, None),
    # License
    "91504": (None, None),
    # Pilot License Number
    "1639": (None, None),
    # I.F.E.
    "91712": (None, None),
    # Enterprise Number
    "91720": (None, None),
    # Branch Unit Number
    "91719": (None, None),
    # Trade License No.
    "1615": (None, None),
    # Citizen's Card Number
    "91739": ("LegalEntity", "idNumber"),
    # UAE Identification
    "91740": ("LegalEntity", "idNumber"),
    # Business Registration Number
    "91760": ("LegalEntity", "registrationNumber"),
    # Tarjeta Profesional
    "91750": (None, None),
    # United Social Credit Code Certificate (USCCC)
    "91747": (None, None),
    # Chamber of Commerce Number
    "91751": (None, None),
    # Legal Entity Number
    "91752": ("LegalEntity", "registrationNumber"),
    # Business Number
    "91753": ("LegalEntity", "registrationNumber"),
    # Birth Certificate Number
    "91759": (None, None),
    # RSIN
    "91813": (None, None),
    # MSB Registration Number
    "91812": (None, None),
    # File Number
    "91835": (None, None),
    # C.U.I.
    "91854": (None, None),
    # Aircraft Serial Identification
    "1623": ("Vehicle", "registrationNumber"),
    # Seafarer's Identification Document
    "91891": (None, None),
    # Tourism License No.
    "1621": (None, None),
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


def parse_date(node):
    parts = (
        node.findtext("./Year"),
        node.findtext("./Month"),
        node.findtext("./Day"),
    )
    return "-".join(parts)


def date_prefix(*dates):
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
    start_from = parse_date(start.find("./From"))
    start_to = parse_date(start.find("./To"))
    end = date.find("./End")
    end_from = parse_date(end.find("./From"))
    end_to = parse_date(end.find("./To"))
    return (
        date_prefix(start_from, start_to),
        date_prefix(end_from, end_to),
    )


def load_locations(doc):
    locations = {}
    for location in doc.findall("./Locations/Location"):
        location_id = location.get("ID")
        countries = set()
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

        countries = set([parts.get("Unknown")])
        for area in location.findall("./LocationAreaCode"):
            area_code = ref_get("AreaCode", area.get("AreaCodeID"))
            country = ref_get("Country", area_code.get("CountryID"))
            countries.add(country.get("ISO2"))

        for country in location.findall("./LocationCountry"):
            country = ref_get("Country", country.get("CountryID"))
            countries.add(country.get("ISO2"))

        locations[location_id] = (address, countries)
    return locations


def load_documents(doc):
    documents = {}
    for regdoc in doc.findall("./IDRegDocuments/IDRegDocument"):
        identity_id = regdoc.get("IdentityID")
        documents.setdefault(identity_id, [])
        documents[identity_id].append(regdoc)
    return documents


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


def parse_party(emitter, doc, distinct_party, locations, documents):
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

        for regdoc in documents.get(identity.get("ID"), []):
            authority = regdoc.findtext("./IssuingAuthority")
            number = regdoc.findtext("./IDRegistrationNo")
            doc_type_id = regdoc.get("IDRegDocTypeID")
            doc_type = ref_value("IDRegDocType", doc_type_id)
            country = regdoc.get("IssuedBy-CountryID")
            if country is not None:
                country = ref_get("Country", country).get("ISO2")
            party.add("country", country)

            if authority == "INN":
                party.add("innCode", number)
                continue
            if authority == "OGRN":
                party.schema = model.get("Company")
                party.add("ogrnCode", number)
                continue

            reg_schema, prop = REG_ID[doc_type_id]
            if reg_schema is None:
                if not party.schema.is_a("LegalEntity"):
                    continue
                passport = emitter.make("Passport")
                passport.make_id("Passport", party.id, regdoc.get("ID"))
                passport.add("holder", party)
                passport.add("type", doc_type)
                passport.add("country", country)
                passport.add("passportNumber", number)
                passport.add("authority", authority)
                emitter.emit(passport)
                continue

            if not disjoint_schema(party, reg_schema):
                add_schema(party, reg_schema)
                party.add(prop, number)
                continue

            # if doc_type_id not in CACHE:
            #     print("    # %s" % doc_type)
            #     print("    '%s': (None, None)," % doc_type_id)
            #     CACHE[doc_type_id] = doc_type

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

        location = feature.find(".//VersionLocation")
        if location is not None:
            address, countries = locations[location.get("LocationID")]
            party.add("address", address)
            party.add("country", countries)

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

    dates = set()
    for event in entry.findall("./EntryEvent"):
        date = parse_date(event.find("./Date"))
        dates.add(date)
        sanction.add("startDate", date)
        sanction.add("summary", event.findtext("./Comment"))
        basis = ref_value("LegalBasis", event.get("LegalBasisID"))
        sanction.add("reason", basis)

    if len(dates):
        party.context["created_at"] = min(dates)
        party.context["updated_at"] = max(dates)

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
    # semantic sense, so we're flipping them here.
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
        context.log.info("Loading reference values...")
        load_ref_values(doc)
        context.log.info("Loading locations...")
        locations = load_locations(doc)
        context.log.info("Loading ID reg documents...")
        documents = load_documents(doc)

        for distinct_party in doc.findall(".//DistinctParty"):
            parse_party(emitter, doc, distinct_party, locations, documents)

        for entry in doc.findall(".//SanctionsEntry"):
            parse_entry(emitter, doc, entry)

        for relation in doc.findall(".//ProfileRelationship"):
            parse_relation(emitter, doc, relation)

    emitter.finalize()
