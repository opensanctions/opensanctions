# cf.
# https://home.treasury.gov/system/files/126/sdn_advanced_notes.pdf
from functools import cache
from banal import first, as_bool
from collections import defaultdict
from typing import Optional, Dict, Union, List, Tuple
from lxml.etree import _Element as Element
from os.path import commonprefix
from prefixdate import parse_parts
from followthemoney import model
from followthemoney.types import registry
from followthemoney.schema import Schema
from followthemoney.exc import InvalidData
from zavod.parse.xml import ElementOrTree

from opensanctions.core import Context, Dataset, Entity
from opensanctions import helpers as h
from opensanctions.helpers.dates import parse_date
from opensanctions.helpers.text import clean_note

FeatureValue = Union[str, Entity]
FeatureValues = List[FeatureValue]


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
ALIAS_TYPES = {
    "Name": "name",
    "A.K.A.": "alias",
    "F.K.A.": "previousName",
    "N.K.A.": "name",
}
URL = "https://sanctionssearch.ofac.treas.gov/Details.aspx?id=%s"
SANCTION_FEATURES = {
    "CAATSA Section 235 Information:": "summary",
    "Executive Order 14024 Directive Information": "summary",
    "Executive Order 14024 Directive Information -": "summary",
    "Executive Order 13662 Directive Determination -": "summary",
    "Executive Order 13846 information:": "summary",
    "Additional Sanctions Information -": "summary",
    "Secondary sanctions risk:": "summary",
    "Transactions Prohibited For Persons Owned or Controlled By U.S. Financial Institutions:": "summary",
    "IFCA Determination -": "summary",
    "PEESA Information:": "summary",
    "Effective Date (CMIC)": "startDate",
    "Purchase/Sales For Divestment Date (CMIC)": "startDate",
    "Effective Date (EO 14024 Directive 1a):": "startDate",
    "Effective Date (EO 14024 Directive 2):": "startDate",
    "Effective Date (EO 14024 Directive 3):": "startDate",
    "Listing Date (EO 14024 Directive 1a):": "listingDate",
    "Listing Date (EO 14024 Directive 2):": "listingDate",
    "Listing Date (EO 14024 Directive 3):": "listingDate",
    "Listing Date (CMIC)": "listingDate",
}


def get_relation_schema(party_schema: Schema, range: Schema) -> Schema:
    if range.is_a("Asset") and party_schema.is_a("Organization"):
        return model.get("Company")
    try:
        model.common_schema(party_schema, range)
        return range
    except InvalidData:
        raise


def get_ref_element(refs: Element, type_: str, id: str) -> Element:
    element = refs.find(f"./{type_}Values/{type_}[@ID='{id}']")
    if element is None:
        raise ValueError("Cannot find reference value [%s]: %s" % (type_, id))
    return element


@cache
def get_ref_text(refs: Element, type_: str, id: str) -> Optional[str]:
    element = get_ref_element(refs, type_, id)
    return element.text


@cache
def get_ref_country(refs: Element, id: str) -> str:
    element = get_ref_element(refs, "Country", id)
    iso2 = element.get("ISO2")
    if iso2 is not None:
        return iso2.lower()
    return element.text


def parse_date(el: Element) -> str:
    pf = parse_parts(
        el.findtext("./Year"),
        el.findtext("./Month"),
        el.findtext("./Day"),
    )
    return pf.text


def date_prefix(*dates: str) -> Optional[str]:
    prefix = commonprefix(dates)[:10]
    if len(prefix) < 10:
        prefix = prefix[:7]
    if len(prefix) < 7:
        prefix = prefix[:4]
    if len(prefix) < 4:
        prefix = None
    return prefix


def parse_date_period(date: Element) -> Tuple[str, ...]:
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


def parse_location(context: Context, refs: Element, location: Element) -> Entity:
    countries = set()
    for area in location.findall("./LocationAreaCode"):
        area_code = get_ref_element(refs, "AreaCode", area.get("AreaCodeID"))
        area_country = registry.country.clean(area_code.get("Description"))
        countries.add(area_country)

    for loc_country in location.findall("./LocationCountry"):
        country = get_ref_country(refs, loc_country.get("CountryID"))
        country_code = registry.country.clean(country)
        countries.add(country_code)

    if len(countries) > 1:
        context.log.warn("Multiple countries", countries=countries)

    parts: Dict[Optional[str], Optional[str]] = {}
    for part in location.findall("./LocationPart"):
        part_type = get_ref_text(refs, "LocPartType", part.get("LocPartTypeID"))
        parts[part_type] = part.findtext("./LocationPartValue/Value")

    country = first(countries)
    unknown = parts.pop("Unknown", None)
    if registry.country.clean(unknown, fuzzy=True):
        country = unknown

    if country == "undetermined":
        country = unknown = None

    address = h.make_address(
        context,
        full=unknown,
        street=parts.pop("ADDRESS1", None),
        street2=parts.pop("ADDRESS2", None),
        street3=parts.pop("ADDRESS3", None),
        city=parts.pop("CITY", None),
        postal_code=parts.pop("POSTAL CODE", None),
        region=parts.pop("REGION", None),
        state=parts.pop("STATE/PROVINCE", None),
        country=country,
        key=location.get("ID"),
    )
    context.audit_data(parts)
    return address


def parse_relation(
    context: Context, refs: Element, el: Element, parties: Dict[str, Schema]
):
    type_id = el.get("RelationTypeID")
    type_ = get_ref_text(refs, "RelationType", type_id)
    from_id = context.make_slug(el.get("From-ProfileID"))
    to_id = context.make_slug(el.get("To-ProfileID"))

    relation = lookup("relations", type_id)
    if relation is None:
        context.log.warn(
            "Unknown relation type",
            type_id=type_id,
            type_value=type_,
            from_id=from_id,
            to_to=to_id,
        )
        return
    entity = context.make(relation.schema)
    from_prop = entity.schema.get(relation.from_prop)
    to_prop = entity.schema.get(relation.to_prop)

    try:
        get_relation_schema(parties[from_id], from_prop.range)
        get_relation_schema(parties[to_id], to_prop.range)
    except InvalidData as exc:
        # HACK: Looks like OFAC just has some link in a direction that makes no
        # semantic sense, so we're flipping them here.
        # context.log.warning(
        #     "Flipped relation",
        #     from_id=from_id,
        #     to_id=to_id,
        #     schema=entity.schema,
        #     type=type_,
        # )
        from_id, to_id = to_id, from_id

    from_party = context.make(get_relation_schema(parties[from_id], from_prop.range))
    from_party.id = from_id
    if not parties[from_id].is_a(from_party.schema):
        context.emit(from_party)

    to_party = context.make(get_relation_schema(parties[to_id], to_prop.range))
    to_party.id = to_id
    if not parties[to_id].is_a(to_party.schema):
        context.emit(to_party)

    entity.id = context.make_id("Relation", from_party.id, to_party.id, el.get("ID"))
    entity.add(from_prop, from_party)
    entity.add(to_prop, to_party)
    entity.add(relation.description_prop, type_)
    entity.add("summary", el.findtext("Comment"))
    context.emit(entity)
    context.log.debug("Relation", from_=from_party, type=type_, to=to_party)
    # pprint(entity.to_dict())


def parse_schema(refs: Element, sub_type_id: str) -> str:
    sub_type = get_ref_element(refs, "PartySubType", sub_type_id)

    type_text = sub_type.text
    if type_text == "Unknown":
        type_text = get_ref_text(refs, "PartyType", sub_type.get("PartyTypeID"))

    return TYPES[type_text]


def parse_distinct_party(
    context: Context, doc: ElementOrTree, refs: Element, party: Element
) -> Entity:
    profiles = party.findall("./Profile")
    assert len(profiles) == 1
    profile = profiles[0]

    schema = parse_schema(refs, profile.get("PartySubTypeID"))
    proxy = context.make(schema)
    profile_id = profile.get("ID")
    proxy.id = context.make_slug(profile_id)
    proxy.add("notes", party.findtext("./Comment"))
    proxy.add("sourceUrl", URL % profile.get("ID"))
    # return proxy

    identities = profile.findall("./Identity")
    assert len(identities) == 1
    identity = identities[0]

    # Name parts (not clear why this cannot be in aliases...)
    parts: Dict[str, str] = {}
    for group in identity.findall("NamePartGroups/MasterNamePartGroup/NamePartGroup"):
        part = get_ref_text(refs, "NamePartType", group.get("NamePartTypeID"))
        parts[group.get("ID")] = part
    assert len(parts), identity

    # Alias names
    for alias in identity.findall("Alias"):
        parse_alias(context, proxy, refs, parts, alias)

    # Registrations and identification documents
    identity_id = identity.get("ID")
    reg_doc_path = f"IDRegDocuments/IDRegDocument[@IdentityID='{identity_id}']"
    for reg_doc in doc.findall(reg_doc_path):
        parse_id_reg_document(context, proxy, refs, reg_doc)

    features: Dict[str, FeatureValues] = {}
    for feature in profile.findall("./Feature"):
        feat_label, values = parse_feature(context, refs, doc, feature)
        if feat_label not in features:
            features[feat_label] = []
        features[feat_label].extend(values)

    # Sanctions designations
    entry_path = f"SanctionsEntries/SanctionsEntry[@ProfileID='{profile_id}']"
    for sanctions_entry in doc.findall(entry_path):
        parse_sanctions_entry(context, proxy, refs, features, sanctions_entry)

    for feat_label, values in features.items():
        for feat_value in values:
            apply_feature(context, proxy, feat_label, feat_value)

    context.emit(proxy, target=True)
    return proxy


def parse_alias(
    context: Context,
    proxy: Entity,
    refs: Element,
    parts: Dict[str, str],
    alias: Element,
) -> None:
    # primary = as_bool(alias.get("Primary"))
    is_weak = as_bool(alias.get("LowQuality"))
    alias_type = get_ref_element(refs, "AliasType", alias.get("AliasTypeID"))
    name_prop = ALIAS_TYPES[alias_type.text]
    for name in alias.findall("DocumentedName"):
        names = defaultdict(lambda: "")
        for value in name.findall("DocumentedNamePart/NamePartValue"):
            type_ = parts.get(value.get("NamePartGroupID"))
            names[type_] = " ".join([names[type_], value.text]).strip()

        h.apply_name(
            proxy,
            full=names.pop("Entity Name", None),
            name_prop=name_prop,
            is_weak=is_weak,
        )
        proxy.add("name", names.pop("Vessel Name", None))
        proxy.add("weakAlias", names.pop("Nickname", None))
        proxy.add("name", names.pop("Aircraft Name", None))
        h.apply_name(
            proxy,
            first_name=names.pop("First Name", None),
            middle_name=names.pop("Middle Name", None),
            maiden_name=names.pop("Maiden Name", None),
            last_name=names.pop("Last Name", None),
            matronymic=names.pop("Matronymic", None),
            patronymic=names.pop("Patronymic", None),
            is_weak=is_weak,
            name_prop=name_prop,
        )
        context.audit_data(names)


def parse_id_reg_document(
    context: Context, proxy: Entity, refs: Element, reg_doc: Element
) -> None:
    authority = reg_doc.findtext("IssuingAuthority")
    number = reg_doc.findtext("IDRegistrationNo")
    comment = reg_doc.findtext("Comment")
    doc_type_id = reg_doc.get("IDRegDocTypeID")
    doc_type = get_ref_element(refs, "IDRegDocType", doc_type_id)
    conf = lookup("doc_types", doc_type.text)
    if conf is None:
        context.log.warning(
            "Unmapped reg doc type",
            type=doc_type.text,
            number=number,
            authority=authority,
        )
        return

    country = reg_doc.get("IssuedBy-CountryID")
    if country is not None:
        country = get_ref_country(refs, country)
    proxy.add("country", country)

    if conf.prop is not None:
        proxy.add(conf.prop, number)

    if conf.identification or conf.passport:
        issue_date = None
        expire_date = None
        for date in reg_doc.findall("./DocumentDate"):
            period = parse_date_period(date.find("./DatePeriod"))
            date_type_id = date.get("IDRegDocDateTypeID")
            date_type = get_ref_element(refs, "IDRegDocDateType", date_type_id)
            if date_type.text == "Issue Date":
                issue_date = period
            elif date_type.text == "Expiration Date":
                expire_date = period
            else:
                context.log.warning(
                    "Unknown document date type", date_type=date_type.text
                )

        identification = h.make_identification(
            context,
            proxy,
            number=number,
            doc_type=doc_type.text,
            country=country,
            summary=comment,
            start_date=issue_date,
            end_date=expire_date,
            authority=authority,
            passport=conf.passport,
            key=reg_doc.get("ID"),
        )
        if identification is None:
            return
        context.emit(identification)


def parse_sanctions_entry(
    context: Context,
    proxy: Entity,
    refs: Element,
    features: Dict[str, FeatureValues],
    entry: Element,
) -> Entity:
    # context.inspect(entry)
    proxy.add("topics", "sanction")
    sanction = h.make_sanction(context, proxy, key=entry.get("ID"))
    sanction.set("program", get_ref_text(refs, "List", entry.get("ListID")))
    sanction.set("authorityId", entry.get("ProfileID"))

    for event in entry.findall("./EntryEvent"):
        sanction.add("summary", event.findtext("./Comment"))
        basis = get_ref_text(refs, "LegalBasis", event.get("LegalBasisID"))
        sanction.add("reason", basis)

    for measure in entry.findall("./SanctionsMeasure"):
        sanctions_type_id = measure.get("SanctionsTypeID")
        sanctions_type = get_ref_text(refs, "SanctionsType", sanctions_type_id)
        if sanctions_type == "Program":
            sanctions_type = measure.findtext("Comment")
        sanction.add("provisions", sanctions_type)

    for feature, prop in SANCTION_FEATURES.items():
        for value in features.get(feature, []):
            if prop == "summary":
                fname = feature.rstrip(":").rstrip("-").strip()
                value = f"{fname}: {value}"
            sanction.add(prop, value)

    context.emit(sanction)
    return sanction


def parse_feature(
    context: Context, refs: Element, doc: Element, feature: Element
) -> Tuple[str, FeatureValues]:
    """Extract the value of typed features linked to entities."""
    feature_id = feature.get("FeatureTypeID")
    feature_label = get_ref_text(refs, "FeatureType", feature_id).strip()
    values: FeatureValues = []

    version_loc = feature.find(".//VersionLocation")
    if version_loc is not None:
        location_id = version_loc.get("LocationID")
        location = doc.find(f"Locations/Location[@ID='{location_id}']")
        values.append(parse_location(context, refs, location))

    period = feature.find(".//DatePeriod")
    if period is not None:
        values.extend(parse_date_period(period))

    detail = feature.find(".//VersionDetail")
    if detail is not None:
        # detail_type = ref_value("DetailType", detail.get("DetailTypeID"))
        reference_id = detail.get("DetailReferenceID")
        if reference_id is not None:
            value = get_ref_text(refs, "DetailReference", reference_id)
        else:
            value = detail.text
        if value is not None:
            values.append(value)

    if not len(values):
        context.log.warning(
            "Could not extract feature value",
            feature=feature,
            feature_label=feature_label,
        )

    return feature_label, values


def apply_feature(
    context: Context,
    proxy: Entity,
    feature: str,
    value: FeatureValue,
):
    result = lookup("features", feature)
    if result is None:
        context.log.warning(
            "Missing feature",
            entity=proxy,
            schema=proxy.schema,
            feature=repr(feature),
            value=repr(value),
        )
        return

    if result.schema is not None:
        # The presence of this feature implies that the entity has a
        # certain schema.
        proxy.add_schema(result.schema)

    # Handle addresses vs. countries
    if isinstance(value, Entity):
        if feature == "Location":
            h.apply_address(context, proxy, value)
            return
        value = value.first("country")
        if value is None:
            return

    if result.prop is not None:
        # Set a property directly on the entity.
        prop = proxy.schema.get(result.prop)

        # Work-around for airplane jurisdictions:
        if result.prop == "jurisdiction" and proxy.schema.is_a("Vehicle"):
            prop = proxy.schema.get("country")

        if prop is None:
            context.log.warn(
                "Property not found: %s" % result.prop,
                entity=proxy,
                schema=proxy.schema,
                feature=feature,
            )
            return

        if prop.name == "notes":
            value = clean_note(value)

        if prop.name == "dunsCode":
            value = value.strip().replace("-", "")

        proxy.add(prop, value)

    nested: Optional[Dict[str, str]] = result.nested
    if nested is not None:
        # So this is deeply funky: basically, nested entities are
        # mapped from
        adj = context.make(nested.get("schema"))
        adj.id = context.make_id(proxy.id, feature, value)

        value_prop = adj.schema.get(nested.get("value"))
        assert value_prop is not None, nested
        adj.add(value_prop, value)

        if nested.get("feature") is not None:
            feature = feature.replace("Digital Currency Address - ", "")
            adj.add(nested.get("feature"), feature)

        if nested.get("backref") is not None:
            backref_prop = adj.schema.get(nested.get("backref"))
            assert proxy.schema.is_a(backref_prop.range), (
                proxy.schema,
                backref_prop.range,
                feature,
                value,
            )
            adj.add(backref_prop, proxy.id)

        if nested.get("owner"):
            assert proxy.schema.is_a("Asset")
            assert adj.schema.is_a("LegalEntity")
            own = context.make("Ownership")
            own.id = context.make_id("Ownership", proxy.id, adj.id)
            own.add("owner", adj)
            own.add("asset", proxy)
            context.emit(own)

        if adj.schema.is_a("Thing"):
            adj.add("topics", "sanction")
        context.emit(adj)


def crawl(context: Context):
    path = context.fetch_resource("source.xml", context.source.data.url)
    context.export_resource(path, "text/xml", title=context.SOURCE_TITLE)
    doc = context.parse_resource_xml(path)
    doc = h.remove_namespace(doc)
    refs = doc.find("ReferenceValueSets")
    assert refs is not None, "ReferenceValueSets not found"

    # TODO: get last modified date from data

    # Print back a cleaned and formatted version of the source data for debug:
    # from lxml.etree import tostring
    # clean_path = context.get_resource_path("clean.xml")
    # with open(clean_path, "wb") as fh:
    #     fh.write(tostring(doc, pretty_print=True, encoding="utf-8"))

    parties: Dict[str, Schema] = {}
    for distinct_party in doc.findall("./DistinctParties/DistinctParty"):
        proxy = parse_distinct_party(context, doc, refs, distinct_party)
        parties[proxy.id] = proxy.schema

    for relation in doc.findall(".//ProfileRelationship"):
        parse_relation(context, refs, relation, parties)
