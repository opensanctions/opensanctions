# cf.
# https://home.treasury.gov/system/files/126/sdn_advanced_notes.pdf
import re
from pathlib import Path
from functools import cache, lru_cache
from banal import first, as_bool
from collections import defaultdict
from typing import Optional, Dict, Union, List, Tuple, Set
from datapatch import Result
from lxml.etree import tostring, _Element as Element
from os.path import commonprefix
from prefixdate import parse_parts
from followthemoney import model
from followthemoney.types import registry
from followthemoney.schema import Schema
from followthemoney.exc import InvalidData

from zavod import Context, Entity, Dataset
from zavod.meta import load_dataset_from_path
from zavod import helpers as h
from zavod.util import ElementOrTree

FeatureValue = Union[str, Entity, None]
FeatureValues = List[FeatureValue]


@cache
def load_sdn() -> Dataset:
    sdn_path = Path(__file__).parent / "us_ofac_sdn.yml"
    dataset = load_dataset_from_path(sdn_path)
    assert dataset is not None
    return dataset


def lookup(name: str, value: Optional[str]) -> Optional[Result]:
    # We don't want to duplicate the lookup configs in both YAML files,
    # so we're hard-coding that lookups go against the SDN config.
    lookup = load_sdn().lookups.get(name)
    if lookup is None:
        return None
    return lookup.match(value)


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
    "Executive Order 14014 Directive Information:": "summary",
    "Executive Order 13662 Directive Determination -": "summary",
    "Executive Order 13846 information:": "summary",
    "Additional Sanctions Information -": "summary",
    "Secondary sanctions risk:": "summary",
    "Transactions Prohibited For Persons Owned or Controlled By U.S. Financial Institutions:": "summary",  # noqa
    "IFCA Determination -": "summary",
    "PEESA Information:": "summary",
    "Effective Date (CMIC)": "startDate",
    "Purchase/Sales For Divestment Date (CMIC)": "startDate",
    "Effective Date (EO 14024 Directive 1a):": "startDate",
    "Effective Date (EO 14024 Directive 2):": "startDate",
    "Effective Date (EO 14024 Directive 3):": "startDate",
    "Effective Date (EO 14014 Directive 1):": "startDate",
    "Listing Date (EO 14024 Directive 1a):": "listingDate",
    "Listing Date (EO 14024 Directive 2):": "listingDate",
    "Listing Date (EO 14024 Directive 3):": "listingDate",
    "Listing Date (EO 14014 Directive 1):": "listingDate",
    "Listing Date (CMIC)": "listingDate",
}
IMO_RE = re.compile(r"IMO \d{6,9}")


def get_relation_schema(
    party_schema: Optional[Schema], range: Optional[Schema]
) -> Schema:
    assert party_schema is not None
    assert range is not None
    if range.is_a("Asset") and party_schema.is_a("Organization"):
        company = model.get("Company")
        assert company is not None
        return company
    try:
        model.common_schema(party_schema, range)
        return range
    except InvalidData:
        raise


def get_ref_element(refs: Element, type_: str, id: Optional[str]) -> Element:
    if id is None:
        raise ValueError("Reference ID is None for type: %s" % type_)
    element = refs.find(f"./{type_}Values/{type_}[@ID='{id}']")
    if element is None:
        raise ValueError("Cannot find reference value [%s]: %s" % (type_, id))
    return element


@cache
def get_ref_text(refs: Element, type_: str, id: str) -> Optional[str]:
    element = get_ref_element(refs, type_, id)
    return element.text


@cache
def get_ref_country(refs: Element, id: str) -> Optional[str]:
    element = get_ref_element(refs, "Country", id)
    return element.text or element.get("ISO2")


def parse_date(el: Optional[Element]) -> Optional[str]:
    if el is None:
        return None
    pf = parse_parts(
        el.findtext("./Year"),
        el.findtext("./Month"),
        el.findtext("./Day"),
    )
    return pf.text


def date_prefix(*dates: Optional[str]) -> Optional[str]:
    dates_ = [d for d in dates if d is not None]
    prefix: Optional[str] = commonprefix(dates_)[:10]
    if prefix is not None and len(prefix) < 10:
        prefix = prefix[:7]
    if prefix is not None and len(prefix) < 7:
        prefix = prefix[:4]
    if prefix is not None and len(prefix) < 4:
        prefix = None
    return prefix


def parse_date_period(date: Element) -> Tuple[str, ...]:
    start: Optional[str] = None
    start_el = date.find("./Start")
    if start_el is not None:
        start_from = parse_date(start_el.find("./From"))
        start_to = parse_date(start_el.find("./To"))
        start = date_prefix(start_from, start_to)
    end: Optional[str] = None
    end_el = date.find("./End")
    if end_el is not None:
        end_from = parse_date(end_el.find("./From"))
        end_to = parse_date(end_el.find("./To"))
        end = date_prefix(end_from, end_to)
    # This is a little sketchy, but OFAC seems to use date ranges spanning
    # a whole year as a way of signalling a lack of precision:
    if start is not None and end is not None:
        common = date_prefix(start, end)
        if common is not None and len(common) == 4:
            if start.endswith("-01-01") and end.endswith("-12-31"):
                return (common,)
    return tuple([d for d in (start, end) if d is not None])


@lru_cache(maxsize=2000)
def parse_location(context: Context, refs: Element, location: Element) -> FeatureValue:
    countries: Set[Optional[str]] = set()
    for area in location.findall("./LocationAreaCode"):
        area_code = get_ref_element(refs, "AreaCode", area.get("AreaCodeID"))
        countries.add(area_code.get("Description"))

    for loc_country in location.findall("./LocationCountry"):
        country = get_ref_country(refs, loc_country.get("CountryID"))
        countries.add(country)

    country_names = [c for c in countries if c not in ("undetermined", None)]
    if len(country_names) > 1:
        context.log.warn("Multiple countries", countries=country_names)
    country_name = first(country_names)

    parts: Dict[Optional[str], Optional[str]] = {}
    for part in location.findall("./LocationPart"):
        part_type = get_ref_text(refs, "LocPartType", part.get("LocPartTypeID"))
        parts[part_type] = part.findtext("./LocationPartValue/Value")

    unknown = parts.pop("Unknown", None)
    if country_name is None and registry.country.clean(unknown):
        country_name = unknown

    if country_name is None and unknown is not None:
        context.log.warning("Unknown country, but have text", unknown=unknown)

    country_code = registry.country.clean(country_name)
    if country_name is not None and country_code is None:
        context.log.warning(
            "Cannot parse country for location",
            country=country_name,
            parts=parts,
        )

    address = h.make_address(
        context,
        # full=unknown,
        street=parts.pop("ADDRESS1", None),
        street2=parts.pop("ADDRESS2", None),
        street3=parts.pop("ADDRESS3", None),
        city=parts.pop("CITY", None),
        postal_code=parts.pop("POSTAL CODE", None),
        region=parts.pop("REGION", None),
        state=parts.pop("STATE/PROVINCE", None),
        country_code=country_code,
        key=location.get("ID"),
    )
    context.audit_data(parts)
    if address is None:
        return country_name
    return address


def parse_relation(
    context: Context, refs: Element, el: Element, parties: Dict[str, Schema]
) -> None:
    type_id = el.get("RelationTypeID")
    type_ = get_ref_text(refs, "RelationType", type_id)
    from_id = context.make_slug(el.get("From-ProfileID"))
    if from_id is None or from_id not in parties:
        context.log.warn(
            "Unknown from party",
            type_id=type_id,
            type_value=type_,
            from_id=from_id,
            to_id=el.get("To-ProfileID"),
        )
        return
    to_id = context.make_slug(el.get("To-ProfileID"))
    if to_id is None or to_id not in parties:
        context.log.warn(
            "Unknown to party",
            type_id=type_id,
            type_value=type_,
            from_id=from_id,
            to_id=to_id,
        )
        return

    relation = lookup("relations", type_id)
    if relation is None:
        context.log.warn(
            "Unknown relation type",
            type_id=type_id,
            type_value=type_,
            from_id=from_id,
            to_id=to_id,
        )
        return
    entity = context.make(relation.schema)
    from_prop = entity.schema.get(relation.from_prop)
    if from_prop is None:
        msg = f"Invalid from: {relation.from_prop} for relation {entity.schema.name}"
        raise ValueError(msg)

    to_prop = entity.schema.get(relation.to_prop)
    if to_prop is None:
        msg = f"Invalid to: {relation.to_prop} for relation {entity.schema.name}"
        raise ValueError(msg)

    try:
        get_relation_schema(parties[from_id], from_prop.range)
        get_relation_schema(parties[to_id], to_prop.range)
    except InvalidData:
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
    if not parties[from_id].is_a(from_party.schema) and len(from_party.properties):
        context.emit(from_party)

    to_party = context.make(get_relation_schema(parties[to_id], to_prop.range))
    to_party.id = to_id
    if not parties[to_id].is_a(to_party.schema) and len(to_party.properties):
        context.emit(to_party)

    entity.id = context.make_id("Relation", from_party.id, to_party.id, el.get("ID"))
    entity.add(from_prop, from_party)
    entity.add(to_prop, to_party)
    entity.add(relation.description_prop, type_)
    entity.add("summary", el.findtext("Comment"))
    context.emit(entity)
    context.log.debug("Relation", from_=from_party, type=type_, to=to_party)
    # pprint(entity.to_dict())


def parse_schema(refs: Element, sub_type_id: Optional[str]) -> str:
    sub_type = get_ref_element(refs, "PartySubType", sub_type_id)

    type_text = sub_type.text
    if type_text == "Unknown":
        type_text = get_ref_text(refs, "PartyType", sub_type.get("PartyTypeID"))

    if type_text not in TYPES:
        raise ValueError("Unknown party type: %s" % type_text)

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
        group_id = group.get("ID")
        if group_id is None or part is None:
            context.log.warning("Invalid name part group", group=tostring(group))
            continue
        parts[group_id] = part
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
    # We skip emitting entries with ListId "Consolidated List" because they always have a second,
    # more specific ListId attached (such as "CAPTA List"). This assert ensures that this assumption
    # always holds so that we don't skip emitting a Sanction for every entity on the list.
    sanction_entities = [
        emit_sanctions_entry(context, proxy, refs, features, sanctions_entry)
        for sanctions_entry in doc.findall(entry_path)
    ]
    sanction_found = any(sanction is not None for sanction in sanction_entities)
    assert sanction_found

    for feat_label, values in features.items():
        for feat_value in values:
            apply_feature(context, proxy, feat_label, feat_value)

    context.emit(proxy)
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
    if alias_type.text not in ALIAS_TYPES:
        raise ValueError("Unknown alias type: %s" % alias_type.text)
    name_prop = ALIAS_TYPES[alias_type.text]
    for name in alias.findall("DocumentedName"):
        names: Dict[str, str] = defaultdict(lambda: "")
        lang = "eng"
        for value in name.findall("DocumentedNamePart/NamePartValue"):
            script_id = value.get("ScriptID")
            if script_id is not None and script_id != "215":  # Latin
                script = get_ref_element(refs, "Script", script_id)
                script_code = script.get("ScriptCode")
                script_lang = context.lookup_value("script.lang", script_code)
                if lang == "eng" and script_lang is not None:
                    lang = script_lang
                elif lang != script_lang:
                    context.log.warning(
                        "Conflicting name languages",
                        name=name,
                        lang1=lang,
                        lang2=script_lang,
                    )
            name_part_group_id = value.get("NamePartGroupID")
            if name_part_group_id is None:
                context.log.warning("Missing name part group ID", value=tostring(value))
                continue
            type_ = parts.get(name_part_group_id)
            if type_ is None:
                context.log.warning(
                    "Unknown name part type",
                    name_part_group_id=name_part_group_id,
                    value=tostring(value),
                )
                continue
            values = [v for v in (names[type_], value.text) if v is not None]
            names[type_] = " ".join(values).strip()

        h.apply_name(
            proxy,
            full=names.pop("Entity Name", None),
            name_prop=name_prop,
            is_weak=is_weak,
            lang=lang,
        )
        proxy.add("name", names.pop("Vessel Name", None), lang=lang)
        proxy.add("name", names.pop("Aircraft Name", None), lang=lang)
        h.apply_name(
            proxy,
            prefix=names.pop("Nickname", None),
            first_name=names.pop("First Name", None),
            middle_name=names.pop("Middle Name", None),
            maiden_name=names.pop("Maiden Name", None),
            last_name=names.pop("Last Name", None),
            matronymic=names.pop("Matronymic", None),
            patronymic=names.pop("Patronymic", None),
            is_weak=is_weak,
            name_prop=name_prop,
            lang=lang,
        )
        context.audit_data(names)


def parse_id_reg_document(
    context: Context, proxy: Entity, refs: Element, reg_doc: Element
) -> None:
    authority = reg_doc.findtext("IssuingAuthority")
    number = reg_doc.findtext("IDRegistrationNo")
    comment = reg_doc.findtext("Comment")
    doc_type_id = reg_doc.get("IDRegDocTypeID")
    assert doc_type_id is not None
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

    if number is None or number.strip() == "":
        context.log.warning(
            "Missing document number",
            entity=proxy.id,
            type=doc_type.text,
            authority=authority,
        )
        return

    country = reg_doc.get("IssuedBy-CountryID")
    if country is not None:
        country = get_ref_country(refs, country)
    proxy.add("country", country)

    if conf.prop is not None:
        if IMO_RE.match(number):
            proxy.add("imoNumber", number)
        else:
            proxy.add(conf.prop, number)

    if proxy.schema.is_a("Person") and (conf.identification or conf.passport):
        issue_date: Optional[str] = None
        expire_date: Optional[str] = None
        for date in reg_doc.findall("./DocumentDate"):
            period_el = date.find("./DatePeriod")
            assert period_el is not None
            period = parse_date_period(period_el)
            date_type_id = date.get("IDRegDocDateTypeID")
            date_type = get_ref_element(refs, "IDRegDocDateType", date_type_id)

            # Identity issue dates are expressed in the data as time periods with equal start and end dates.
            # If that assumption breaks, let's warn to have someone investigate.
            if len(set(period)) != 1:
                context.log.warning(
                    "Identity issue/expiration date has multiple different dates, that's unexpected, please investigate.",
                    entity=proxy.id,
                    period=period,
                )

            if date_type.text == "Issue Date":
                issue_date = min(period)
            elif date_type.text == "Expiration Date":
                expire_date = max(period)
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


def extract_sdn_sanctions_measure_name(entry: Element, refs: Element) -> Optional[str]:
    """
    Extracts the program name associated with a sanctions entry on the SDN List.

    Returns:
        A string representing the program name (e.g., "IRAN-EO13876")
    """
    list_name = get_ref_text(refs, "List", entry.get("ListID"))
    assert list_name == "SDN List"

    # The SDN List may include multiple SanctionsMeasures, and the actual program name is inside
    # the <Comment> tag of a <SanctionsMeasure> element where SanctionsTypeID resolves to "Program".
    for measure in entry.findall("./SanctionsMeasure"):
        sanctions_type_id = measure.get("SanctionsTypeID")
        sanctions_type = get_ref_text(refs, "SanctionsType", sanctions_type_id)
        if sanctions_type == "Program":
            # Extract the actual program name from the <Comment> element, which holds values like "IRAN-EO13876"
            comment = measure.findtext("Comment")
            if comment:
                return comment  # Return the first matched program name
    # If no "Program" type measures are found or no comment is available, return None
    return None


def emit_sanctions_entry(
    context: Context,
    proxy: Entity,
    refs: Element,
    features: Dict[str, FeatureValues],
    entry: Element,
) -> Optional[Entity]:
    # context.inspect(entry)
    proxy.add("topics", "sanction")

    dataset = context.dataset.name
    list_id = get_ref_text(refs, "List", entry.get("ListID"))
    # For entries on the SDN list, the XML contains a more specific sanctions program designation
    # For the various lists that are part of the Consolidated List, we use the list name as the program.
    program = (
        extract_sdn_sanctions_measure_name(entry, refs)
        if list_id == "SDN List"
        else list_id
    )
    # For us_ofac_sdn, only process entries with list_id 'SDN List'
    if dataset == "us_ofac_sdn" and list_id != "SDN List":
        return None
    # For us_ofac_cons, only process entries that are not part of the SDN List
    # (i.e., process entries with list_id 'Non-SDN Menu-Based Sanctions List').
    # 'Consolidated List' is more of a meta-tag — all entries labeled with it also have a
    # more specific list_id attached (e.g., 'Non-SDN CMIC List'). We have a check
    # aearlier that ensures at least one specific Sanction will be emitted for each entity.
    # Therefore, it's safe to skip processing entries under 'Consolidated List' list_id here.
    if dataset == "us_ofac_cons" and list_id in {
        "Consolidated List",
        "SDN List",
    }:
        return None
    sanction = h.make_sanction(
        context,
        proxy,
        key=entry.get("ID"),
        program_name=program,
        source_program_key=program,
        # For entries on the SDN list, the XML contains a more specific sanctions program designation
        # For the various lists that are part of the Consolidated List, we use the list name as the program.
        program_key=(
            h.lookup_sanction_program_key(context, program) if program else None
        ),
    )
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
    context: Context, refs: Element, doc: ElementOrTree, feature: Element
) -> Tuple[str, FeatureValues]:
    """Extract the value of typed features linked to entities."""
    feature_id = feature.get("FeatureTypeID")
    feature_label = get_ref_text(refs, "FeatureType", feature_id)
    if feature_label is None:
        raise ValueError("Unknown feature type ID: %s" % feature_id)
    feature_label = feature_label.strip()
    values: FeatureValues = []

    version_loc = feature.find(".//VersionLocation")
    if version_loc is not None:
        location_id = version_loc.get("LocationID")
        location = doc.find(f"Locations/Location[@ID='{location_id}']")
        if location is not None:
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
) -> None:
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

    if value is None:
        context.log.warning(
            "Feature value is None",
            entity=proxy,
            schema=proxy.schema,
            feature=feature,
        )
        return

    # Handle addresses vs. countries
    if isinstance(value, Entity):
        if feature == "Location":
            h.apply_address(context, proxy, value)
            proxy.add("address", value.get("full"))
            return
        value = value.first("country")

    if value is None:
        context.log.warning(
            "Feature value is None after processing",
            entity=proxy,
            schema=proxy.schema,
            feature=feature,
        )
        return

    if result.prop is not None:
        # Set a property directly on the entity.
        prop = proxy.schema.get(result.prop)

        # Work-around for airplane jurisdictions:
        if result.prop == "jurisdiction" and proxy.schema.is_a("Vehicle"):
            prop = proxy.schema.get("country")

        # Handle locations which only specify a country:
        if result.prop == "addressEntity" and isinstance(value, str):
            prop = proxy.schema.get("country")

        if prop is None:
            context.log.warn(
                "Property not found: %s" % result.prop,
                entity=proxy,
                schema=proxy.schema,
                feature=feature,
            )
            return

        if prop.name == "dunsCode":
            value = value.strip().replace("-", "")

        values = [value]
        if prop.name == "notes":
            values = h.clean_note(value)

        proxy.add(prop, values)

    nested: Optional[Dict[str, str]] = result.nested
    if nested is not None:
        # So this is deeply funky: basically, nested entities are
        # mapped from
        nested_schema = nested.get("schema")
        assert nested_schema is not None, nested
        adj = context.make(nested_schema)
        adj.id = context.make_id(proxy.id, feature, value)

        value_prop_name = nested.get("value")
        assert value_prop_name is not None, nested
        value_prop = adj.schema.get(value_prop_name)
        assert value_prop is not None, nested
        adj.add(value_prop, value)

        nested_feature = nested.get("feature")
        if nested_feature is not None:
            feature = feature.replace("Digital Currency Address - ", "")
            adj.add(nested_feature, feature)

        backref = nested.get("backref")
        if backref is not None:
            backref_prop = adj.schema.get(backref)
            if backref_prop is None:
                raise ValueError("Backref prop not found: %s" % backref)
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


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.xml", context.data_url)
    context.export_resource(path, "text/xml", title=context.SOURCE_TITLE)
    doc_ = context.parse_resource_xml(path)
    doc = h.remove_namespace(doc_)
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
        if proxy.id is not None:
            parties[proxy.id] = proxy.schema

    for relation in doc.findall(".//ProfileRelationship"):
        parse_relation(context, refs, relation, parties)
