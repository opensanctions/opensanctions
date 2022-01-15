# cf.
# https://github.com/archerimpact/SanctionsExplorer/blob/master/data/sdn_parser.py
# https://home.treasury.gov/system/files/126/sdn_advanced_notes.pdf
from banal import first
from os.path import commonprefix
from followthemoney import model
from followthemoney.types import registry
from followthemoney.exc import InvalidData
from prefixdate import parse_parts

from opensanctions.core import Context, Dataset
from opensanctions import helpers as h
from opensanctions.util import jointext, remove_namespace

REFERENCES = {}


def lookup(name, value):
    # We don't want to duplicate the lookup configs in both YAML files,
    # so we're hard-coding that lookups go against the SDN config.
    sdn = Dataset.require("us_ofac_sdn")
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

URL = "https://sanctionssearch.ofac.treas.gov/Details.aspx?id=%s"


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
    pf = parse_parts(
        el.findtext("./Year"), el.findtext("./Month"), el.findtext("./Day")
    )
    return pf.text


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
    start_el = date.find("./Start")
    start_from = parse_date(start_el.find("./From"))
    start_to = parse_date(start_el.find("./To"))
    end_el = date.find("./End")
    end_from = parse_date(end_el.find("./From"))
    end_to = parse_date(end_el.find("./To"))
    start = date_prefix(start_from, start_to)
    end = date_prefix(end_from, end_to)
    # This is a little sketchy, but OFAC seems to use date ranges spanning
    # a whole year as a way of signalling a lack of precision:
    common = date_prefix(start, end)
    if common is not None and len(common) == 4:
        if start.endswith("-01-01") and end.endswith("-12-31"):
            return (common,)
    return (start, end)


async def load_locations(context: Context, doc):
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
            country = unknown = None

        address = h.make_address(
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
        if address.id is not None:
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


async def parse_feature(context: Context, feature, party, locations):
    feature_id = feature.get("FeatureTypeID")
    location = feature.find(".//VersionLocation")
    if location is not None:
        address = locations.get(location.get("LocationID"))
        await h.apply_address(context, party, address)
        return

    feature_label = ref_value("FeatureType", feature_id)
    value = None

    period = feature.find(".//DatePeriod")
    if period is not None:
        value = parse_date_period(period)
        await h.apply_feature(context, party, feature_label, value)

    detail = feature.find(".//VersionDetail")
    if detail is not None:
        # detail_type = ref_value("DetailType", detail.get("DetailTypeID"))
        reference_id = detail.get("DetailReferenceID")
        if reference_id is not None:
            value = ref_value("DetailReference", reference_id)
        else:
            value = detail.text
        await h.apply_feature(context, party, feature_label, value)


async def parse_registration_doc(context: Context, party, regdoc):
    authority = regdoc.findtext("./IssuingAuthority")
    number = regdoc.findtext("./IDRegistrationNo")
    comment = regdoc.findtext("./Comment")
    issue_date = None
    expire_date = None
    for date in regdoc.findall("./DocumentDate"):
        period = parse_date_period(date.find("./DatePeriod"))
        date_type_id = date.get("IDRegDocDateTypeID")
        date_type = ref_value("IDRegDocDateType", date_type_id)
        if date_type == "Issue Date":
            issue_date = period
        if date_type == "Expiration Date":
            expire_date = period
    country = regdoc.get("IssuedBy-CountryID")
    if country is not None:
        country = ref_get("Country", country).get("ISO2")
        party.add("country", country)

    doc_type_id = regdoc.get("IDRegDocTypeID")
    feature = ref_value("IDRegDocType", doc_type_id)
    if authority in ("INN", "OGRN", "IMO"):
        feature = authority

    await h.apply_feature(
        context,
        party,
        feature,
        number,
        country=country,
        start_date=issue_date,
        end_date=expire_date,
        comment=comment,
        authority=authority,
    )


async def parse_party(context: Context, distinct_party, locations, documents):
    profile = distinct_party.find("Profile")
    sub_type = ref_get("PartySubType", profile.get("PartySubTypeID"))
    schema = TYPES.get(sub_type.get("Value"))
    type_ = ref_value("PartyType", sub_type.get("PartyTypeID"))
    schema = TYPES.get(type_, schema)
    if schema is None:
        context.log.error("Unknown party type", value=type_)
        return
    party = context.make(schema)
    party.id = context.make_slug(profile.get("ID"))
    party.add("notes", distinct_party.findtext("Comment"))
    party.add("sourceUrl", URL % profile.get("ID"))
    party.add("topics", "sanction")

    for identity in profile.findall("./Identity"):
        parts = {}
        for group in identity.findall(".//NamePartGroup"):
            type_id = group.get("NamePartTypeID")
            parts[group.get("ID")] = ref_value("NamePartType", type_id)

        for alias in identity.findall("./Alias"):
            parse_alias(party, parts, alias)

        for regdoc in documents.get(identity.get("ID"), []):
            await parse_registration_doc(context, party, regdoc)

    for feature in profile.findall("./Feature"):
        await parse_feature(context, feature, party, locations)

    context.emit(party, target=True, unique=True)
    # pprint(party.to_dict())
    # context.log.info("[%s] %s" % (party.schema.name, party.caption))
    return party


async def parse_entry(context: Context, entry):
    party = context.make("Thing")
    party.id = context.make_slug(entry.get("ProfileID"))

    sanction = h.make_sanction(context, party, key=entry.get("ID"))
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


async def parse_relation(context: Context, el, parties):
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

    # if type_id == "15003":
    #     print(
    #         "REL",
    #         from_party.caption,
    #         from_party.schema.name,
    #         type_,
    #         to_party.caption,
    #         to_party.schema.name,
    #     )

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
        # context.log.warning(
        #     "Flipped relation",
        #     from_party=from_party,
        #     to_party=to_party,
        #     schema=entity.schema,
        #     type=type_,
        # )
        from_party, to_party = to_party, from_party

    add_schema(from_party, from_range)
    add_schema(to_party, to_range)
    context.emit(from_party, target=True)
    context.emit(to_party, target=True)
    entity.id = context.make_id("Relation", from_party.id, to_party.id, el.get("ID"))
    entity.add(relation.from_prop, from_party)
    entity.add(relation.to_prop, to_party)
    entity.add(relation.description_prop, type_)
    entity.add("summary", el.findtext("./Comment"))
    context.emit(entity)
    context.log.debug("Relation", from_=from_party, type=type_, to=to_party)
    # pprint(entity.to_dict())


async def crawl(context: Context):
    path = context.fetch_resource("source.xml", context.dataset.data.url)
    context.export_resource(path, "text/xml", title=context.SOURCE_TITLE)
    doc = context.parse_resource_xml(path)
    doc = remove_namespace(doc)
    context.log.info("Loading reference values...")
    load_ref_values(doc)
    context.log.info("Loading locations...")
    locations = await load_locations(context, doc)
    context.log.info("Loading ID reg documents...")
    documents = load_documents(doc)

    parties = {}
    for distinct_party in doc.findall(".//DistinctParty"):
        party = await parse_party(context, distinct_party, locations, documents)
        parties[party.id] = party

    for entry in doc.findall(".//SanctionsEntry"):
        await parse_entry(context, entry)

    for relation in doc.findall(".//ProfileRelationship"):
        await parse_relation(context, relation, parties)
