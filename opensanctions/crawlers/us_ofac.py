# cf.
# https://github.com/archerimpact/SanctionsExplorer/blob/master/data/sdn_parser.py
# https://home.treasury.gov/system/files/126/sdn_advanced_notes.pdf
from typing import Optional, Dict, Any, Union, List, Tuple
from banal import first, as_bool
from banal import ensure_list
from followthemoney.types import registry

from collections import defaultdict
from functools import cache
from lxml.etree import _Element as Element
from lxml.etree import tostring
from os.path import commonprefix
from followthemoney import model
from followthemoney.types import registry
from followthemoney.exc import InvalidData
from prefixdate import parse_parts
from zavod.parse.xml import ElementOrTree

from opensanctions.core import Context, Dataset, Entity
from opensanctions import helpers as h
from opensanctions.core.lookups import common_lookups
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


def parse_date(el: Element):
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
    h.audit_data(parts)
    return address


def parse_relation(context: Context, el, parties):
    type_id = el.get("RelationTypeID")
    type_ = ref_value("RelationType", el.get("RelationTypeID"))
    from_id = context.make_slug(el.get("From-ProfileID"))
    from_party = parties.get(from_id)
    if from_party is None:
        context.log.warn("Missing relation 'from' party", entity_id=from_id, type=type_)
        return
    to_id = context.make_slug(el.get("To-ProfileID"))
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
        parse_alias(proxy, refs, parts, alias)

    # Registrations and identification documents
    identity_id = identity.get("ID")
    reg_doc_path = f"IDRegDocuments/IDRegDocument[@IdentityID='{identity_id}']"
    for reg_doc in doc.findall(reg_doc_path):
        parse_id_reg_document(context, proxy, refs, reg_doc)

    # Sanctions designations
    entry_path = f"SanctionsEntries/SanctionsEntry[@ProfileID='{profile_id}']"
    for sanctions_entry in doc.findall(entry_path):
        parse_sanctions_entry(context, proxy, refs, sanctions_entry)

    features: Dict[str, FeatureValues] = {}
    for feature in profile.findall("./Feature"):
        feat_label, values = parse_feature(context, refs, doc, feature)
        if feat_label not in features:
            features[feat_label] = []
        features[feat_label].extend(values)

    for feat_label, values in features.items():
        for feat_value in values:
            apply_feature(context, proxy, feat_label, feat_value)

    # from pprint import pprint
    # pprint(features)
    # print(proxy.to_dict())

    context.emit(proxy, target=True)
    return proxy


def parse_alias(
    proxy: Entity, refs: Element, parts: Dict[str, str], alias: Element
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
        h.audit_data(names)


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
        # context.inspect(identification)
        context.emit(identification)


def parse_sanctions_entry(
    context: Context, proxy: Entity, refs: Element, entry: Element
) -> None:
    sanction = h.make_sanction(context, proxy, key=entry.get("ID"))
    list = get_ref_text(refs, "List", entry.get("ListID"))
    sanction.add("program", list)

    # for event in entry.findall("./EntryEvent"):
    #     date = parse_date(event.find("./Date"))
    #     sanction.add("listingDate", date)
    #     # party.add("createdAt", date)
    #     sanction.add("summary", event.findtext("./Comment"))
    #     basis = ref_value("LegalBasis", event.get("LegalBasisID"))
    #     sanction.add("reason", basis)

    # party.add("topics", "sanction")
    # # sanction.add("listingDate", party.get("createdAt"))
    # # sanction.add("startDate", party.get("modifiedAt"))

    # for measure in entry.findall("./SanctionsMeasure"):
    #     sanction.add("summary", measure.findtext("./Comment"))
    #     type_id = measure.get("SanctionsTypeID")
    #     sanction.add("program", ref_value("SanctionsType", type_id))

    # context.emit(sanction)
    # pprint(sanction.to_dict())


def parse_feature(
    context: Context, refs: Element, doc: Element, feature: Element
) -> Tuple[str, FeatureValues]:
    """Extract the value of typed features linked to entities."""
    feature_id = feature.get("FeatureTypeID")
    feature_label = get_ref_text(refs, "FeatureType", feature_id)
    values: FeatureValues = []

    version_loc = feature.find(".//VersionLocation")
    if version_loc is not None:
        location_id = version_loc.get("LocationID")
        location = doc.find(f"Locations/Location[@ID='{location_id}']")
        address = parse_location(context, refs, location)
        values.append(address)
        # TODO: handle country prop assignments
        # if address.id is not None:
        #     context.emit(address)

    period = feature.find(".//DatePeriod")
    if period is not None:
        value = parse_date_period(period)
        if value is not None:
            values.extend(value)

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
        context.log.warning("Could not extract feature value", feature=feature)

    return feature_label, values


# def _prepare_value(prop, values, date_formats):
#     prepared = []
#     for value in ensure_list(values):

#         if prop.type == registry.date:
#             prepared.extend(parse_date(value, date_formats))
#             continue
#         prepared.append(value)
#     return prepared


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
            feature=feature,
            value=value,
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

        if prop.name == "notes":
            value = clean_note(value)

        if prop.name == "dunsCode":
            value = value.strip().replace("-", "")

        if prop is None:
            context.log.warn(
                "Property not found: %s" % result.prop,
                entity=proxy,
                schema=proxy.schema,
                feature=feature,
            )
        else:
            proxy.add(prop, value)

    nested: Optional[Dict[str, str]] = result.nested
    if nested is not None:
        # So this is deeply funky: basically, nested entities are
        # mapped from
        adj = context.make(nested.pop("schema"))
        adj.id = context.make_id(proxy.id, feature, value)

        value_prop = adj.schema.get(nested.pop("value"))
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

        if nested.get("forwardref") is not None:
            proxy.add(nested.get("forwardref"), adj.id)

        context.emit(adj)


def crawl(context: Context):
    path = context.fetch_resource("source.xml", context.source.data.url)
    context.export_resource(path, "text/xml", title=context.SOURCE_TITLE)
    doc = context.parse_resource_xml(path)
    doc = h.remove_namespace(doc)
    refs = doc.find("ReferenceValueSets")
    assert refs is not None, "ReferenceValueSets not found"

    # Print back a cleaned and formatted version of the source data for debug:
    # from lxml.etree import tostring
    # clean_path = context.get_resource_path("clean.xml")
    # with open(clean_path, "wb") as fh:
    #     fh.write(tostring(doc, pretty_print=True, encoding="utf-8"))

    for distinct_party in doc.findall("./DistinctParties/DistinctParty"):
        parse_distinct_party(context, doc, refs, distinct_party)

    # for relation in doc.findall(".//ProfileRelationship"):
    #     parse_relation(context, relation, parties)
