# cf.
# https://github.com/archerimpact/SanctionsExplorer/blob/master/data/sdn_parser.py
# https://home.treasury.gov/system/files/126/sdn_advanced_notes.pdf
from os.path import commonprefix
from followthemoney import model
from followthemoney.exc import InvalidData

from opensanctions.core.dataset import Dataset
from opensanctions.util import jointext, date_parts, remove_namespace

REFERENCES = {}


def lookup(name, value):
    # We don't want to duplicate the lookup configs in both YAML files,
    # so we're hard-coding that lookups go against the SDN config.
    sdn = Dataset.get("us_ofac_sdn")
    return sdn.lookups.get(name).match(value)


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
        # FIXME: this might make vessels out of companies!!!
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


def parse_date(el):
    return date_parts(
        el.findtext("./Year"), el.findtext("./Month"), el.findtext("./Day")
    )


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


def make_adjacent(context, name):
    entity = context.make("LegalEntity")
    entity.make_slug("named", name)
    entity.add("name", name)
    context.emit(entity)
    return entity


def parse_party(context, doc, distinct_party, locations, documents):
    profile = distinct_party.find("Profile")
    sub_type = ref_get("PartySubType", profile.get("PartySubTypeID"))
    schema = TYPES.get(sub_type.get("Value"))
    type_ = ref_value("PartyType", sub_type.get("PartyTypeID"))
    schema = TYPES.get(type_, schema)
    if schema is None:
        context.log.error("Unknown party type", value=type_)
        return
    party = context.make(schema)
    party.make_slug(profile.get("ID"))
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
            doc_label = ref_value("IDRegDocType", doc_type_id)

            doc_res = lookup("IDRegDocType", doc_type_id)
            if doc_res is None:
                context.log.warn(
                    "Unknown IDRegDocType",
                    id=doc_type_id,
                    label=doc_label,
                )
                continue

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

            if doc_res.schema is None:
                if not party.schema.is_a("LegalEntity"):
                    context.log.warn(
                        "Cannot attach passport",
                        entity=party,
                        id=doc_type_id,
                        label=doc_label,
                    )
                    continue
                passport = context.make("Passport")
                passport.make_id("Passport", party.id, regdoc.get("ID"))
                passport.add("holder", party)
                passport.add("type", doc_label)
                passport.add("country", country)
                passport.add("passportNumber", number)
                passport.add("authority", authority)
                context.emit(passport)
                continue

            if not disjoint_schema(party, doc_res.schema):
                add_schema(party, doc_res.schema)
                party.add(doc_res.prop, number)
                continue

    for feature in profile.findall("./Feature"):
        feature_id = feature.get("FeatureTypeID")
        if feature_id not in FEATURES:
            # context.log.warn(
            #     "Unknown feature type",
            #     id=feature_id,
            #     value=ref_value("FeatureType", feature_id),
            # )
            continue

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
                value = make_adjacent(context, value)
            party.add(prop, value)

    context.emit(party, target=True, unique=True)
    # pprint(party.to_dict())
    # context.log.info("[%s] %s" % (party.schema.name, party.caption))


def parse_entry(context, doc, entry):
    party = context.make("Thing")
    party.make_slug(entry.get("ProfileID"))

    sanction = context.make("Sanction")
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

    context.emit(sanction)
    # pprint(sanction.to_dict())


def parse_relation(context, doc, el):
    type_id = el.get("RelationTypeID")
    store = context.dataset.store
    from_id = context.dataset.make_slug(el.get("From-ProfileID"))
    from_party = store.get(from_id)
    to_id = context.dataset.make_slug(el.get("To-ProfileID"))
    to_party = store.get(to_id)
    type_ = ref_value("RelationType", el.get("RelationTypeID"))
    relation = lookup("relations", type_id)
    if relation is None:
        context.log.warn(
            "Unknown relation type",
            type_id=type_id,
            type_value=type_,
            from_party=from_party,
            to_party=to_party,
        )
        return
    entity = context.make(relation.schema)
    from_range = entity.schema.get(relation.from_prop).range
    to_range = entity.schema.get(relation.to_prop).range

    # HACK: Looks like OFAC just has some link in a direction that makes no
    # semantic sense, so we're flipping them here.
    if disjoint_schema(from_party, from_range) or disjoint_schema(to_party, to_range):
        from_party, to_party = to_party, from_party

    add_schema(from_party, from_range)
    add_schema(to_party, to_range)
    context.emit(from_party)
    context.emit(to_party)
    entity.make_id("Relation", from_party.id, to_party.id, el.get("ID"))
    entity.add(relation.from_prop, from_party)
    entity.add(relation.to_prop, to_party)
    entity.add(relation.description_prop, type_)
    entity.add("summary", el.findtext("./Comment"))
    context.emit(entity)
    context.log.debug("Relation", from_=from_party, type=type_, to=to_party)
    # pprint(entity.to_dict())


def crawl(context):
    context.fetch_artifact("source.xml", context.dataset.data.url)
    doc = context.parse_artifact_xml("source.xml")
    doc = remove_namespace(doc)
    context.log.info("Loading reference values...")
    load_ref_values(doc)
    context.log.info("Loading locations...")
    locations = load_locations(doc)
    context.log.info("Loading ID reg documents...")
    documents = load_documents(doc)

    for distinct_party in doc.findall(".//DistinctParty"):
        parse_party(context, doc, distinct_party, locations, documents)

    for entry in doc.findall(".//SanctionsEntry"):
        parse_entry(context, doc, entry)

    for relation in doc.findall(".//ProfileRelationship"):
        parse_relation(context, doc, relation)
