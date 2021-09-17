# cf.
# https://github.com/archerimpact/SanctionsExplorer/blob/master/data/sdn_parser.py
# https://home.treasury.gov/system/files/126/sdn_advanced_notes.pdf
from banal import first
from os.path import commonprefix
from followthemoney import model
from followthemoney.types import registry
from followthemoney.exc import InvalidData
from prefixdate import parse_parts

from opensanctions.core.dataset import Dataset
from opensanctions.helpers import make_address, apply_address, make_sanction
from opensanctions.util import jointext, remove_namespace

REFERENCES = {}


def lookup(name, value):
    # We don't want to duplicate the lookup configs in both YAML files,
    # so we're hard-coding that lookups go against the SDN config.
    sdn = Dataset.get("us_ofac_sdn")
    return sdn.lookups.get(name).match(value)


TYPES = {
    "Entity": "Organization",
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
    return parse_parts(
        el.findtext("./Year"), el.findtext("./Month"), el.findtext("./Day")
    ).text


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


def load_locations(context, doc):
    locations = {}
    for location in doc.findall("./Locations/Location"):
        location_id = location.get("ID")

        countries = set()
        for area in location.findall("./LocationAreaCode"):
            area_code = ref_get("AreaCode", area.get("AreaCodeID"))
            countries.add(area_code.get("Description"))

        for country in location.findall("./LocationCountry"):
            country_obj = ref_get("Country", country.get("CountryID"))
            countries.add(country_obj.get("Value"))

        if len(countries) > 1:
            context.log.warn("Multiple countries", countries=countries)

        parts = {}
        for part in location.findall("./LocationPart"):
            type_ = ref_value("LocPartType", part.get("LocPartTypeID"))
            parts[type_] = part.findtext("./LocationPartValue/Value")

        country = first(countries)
        unknown = parts.get("Unknown")
        if registry.country.clean(unknown, fuzzy=True):
            country = unknown

        if country == "undetermined":
            country = None

        address = make_address(
            context,
            full=unknown,
            street=parts.get("ADDRESS1"),
            street2=parts.get("ADDRESS2"),
            street3=parts.get("ADDRESS3"),
            city=parts.get("CITY"),
            postal_code=parts.get("POSTAL CODE"),
            region=parts.get("REGION"),
            state=parts.get("STATE/PROVINCE"),
            country=country,
        )
        if address is not None:
            context.emit(address)
            locations[location_id] = address
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


def make_adjacent(context, schema, name):
    entity = context.make(schema)
    entity.make_slug("named", name)
    entity.add("name", name)
    context.emit(entity)
    return entity


def parse_feature(context, feature, party, locations):
    feature_id = feature.get("FeatureTypeID")
    feature_label = ref_value("FeatureType", feature_id)
    feature_res = lookup("FeatureType", int(feature_id))
    if feature_res is None:
        context.log.warn(
            "Unknown FeatureType",
            entity=party,
            id=feature_id,
            value=feature_label,
        )
        return

    if feature_res.schema is not None:
        add_schema(party, feature_res.schema)
    if feature_res.prop is None:
        # from lxml import etree
        # print("---[%r]-> %s" % (party, feature_label))
        # print(etree.tostring(feature).decode("utf-8"))
        return

    period = feature.find(".//DatePeriod")
    if period is not None:
        party.add(feature_res.prop, parse_date_period(period))

    location = feature.find(".//VersionLocation")
    if location is not None:
        address = locations.get(location.get("LocationID"))
        apply_address(context, party, address)

    detail = feature.find(".//VersionDetail")
    if detail is not None:
        # detail_type = ref_value("DetailType", detail.get("DetailTypeID"))
        reference_id = detail.get("DetailReferenceID")
        if reference_id is not None:
            value = ref_value("DetailReference", reference_id)
        else:
            value = detail.text
        if feature_res.entity:
            value = make_adjacent(context, feature_res.entity, value)
        party.add(feature_res.prop, value)


def parse_party(context, distinct_party, locations, documents):
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
                    entity=party,
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

            if doc_res.prop is None:
                if not party.schema.is_a("LegalEntity"):
                    context.log.warn(
                        "Cannot attach passport",
                        entity=party,
                        id=doc_type_id,
                        label=doc_label,
                    )
                    continue
                # TODO: Check out IDRegDocDateType
                passport = context.make("Passport")
                passport.make_id("Passport", party.id, regdoc.get("ID"))
                passport.add("holder", party)
                passport.add("type", doc_label)
                passport.add("country", country)
                passport.add("passportNumber", number)
                passport.add("authority", authority)
                context.emit(passport)
                continue

            # TODO: this should not be there.
            if not disjoint_schema(party, doc_res.schema):
                add_schema(party, doc_res.schema)
                party.add(doc_res.prop, number)
                continue

    for feature in profile.findall("./Feature"):
        parse_feature(context, feature, party, locations)

    context.emit(party, target=True, unique=True)
    # pprint(party.to_dict())
    # context.log.info("[%s] %s" % (party.schema.name, party.caption))
    return party


def parse_entry(context, entry):
    party = context.make("Thing")
    party.make_slug(entry.get("ProfileID"))

    sanction = make_sanction(party, key=entry.get("ID"))
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


def parse_relation(context, el, parties):
    type_id = el.get("RelationTypeID")
    type_ = ref_value("RelationType", el.get("RelationTypeID"))
    from_id = context.dataset.make_slug(el.get("From-ProfileID"))
    from_party = parties.get(from_id)
    if from_party is None:
        context.log.warn("Missing relation 'from' party", entity_id=from_id, type=type_)
        return
    to_id = context.dataset.make_slug(el.get("To-ProfileID"))
    to_party = parties.get(to_id)
    if to_party is None:
        context.log.warn("Missing relation 'to' party", entity_id=to_id, type=type_)
        return
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
    context.emit(from_party, target=True)
    context.emit(to_party, target=True)
    entity.make_id("Relation", from_party.id, to_party.id, el.get("ID"))
    entity.add(relation.from_prop, from_party)
    entity.add(relation.to_prop, to_party)
    entity.add(relation.description_prop, type_)
    entity.add("summary", el.findtext("./Comment"))
    context.emit(entity)
    context.log.debug("Relation", from_=from_party, type=type_, to=to_party)
    # pprint(entity.to_dict())


def crawl(context):
    path = context.fetch_resource("source.xml", context.dataset.data.url)
    context.export_resource(path, "text/xml", title=context.SOURCE_TITLE)
    doc = context.parse_resource_xml(path)
    doc = remove_namespace(doc)
    context.log.info("Loading reference values...")
    load_ref_values(doc)
    context.log.info("Loading locations...")
    locations = load_locations(context, doc)
    context.log.info("Loading ID reg documents...")
    documents = load_documents(doc)

    parties = {}
    for distinct_party in doc.findall(".//DistinctParty"):
        party = parse_party(context, distinct_party, locations, documents)
        parties[party.id] = party

    for entry in doc.findall(".//SanctionsEntry"):
        parse_entry(context, entry)

    for relation in doc.findall(".//ProfileRelationship"):
        parse_relation(context, relation, parties)
