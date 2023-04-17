# cf.
# https://github.com/archerimpact/SanctionsExplorer/blob/master/data/sdn_parser.py
# https://home.treasury.gov/system/files/126/sdn_advanced_notes.pdf
from typing import Optional, Dict
from banal import first, as_bool
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


@cache
def get_ref_value(refs: Element, type_: str, id: str) -> Element:
    element = refs.find(f"./{type_}Values/{type_}[@ID='{id}']")
    if element is None:
        raise ValueError("Cannot find reference value [%s]: %s" % (type_, id))
    return element


@cache
def get_ref_country(refs: Element, id: str) -> str:
    element = get_ref_value(refs, "Country", id)
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


def parse_date_period(date: Element):
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
        area_code = get_ref_value(refs, "AreaCode", area.get("AreaCodeID"))
        area_country = registry.country.clean(area_code.get("Description"))
        countries.add(area_country)

    for loc_country in location.findall("./LocationCountry"):
        country = get_ref_country(refs, loc_country.get("CountryID"))
        country_code = registry.country.clean(country)
        countries.add(country_code)

    if len(countries) > 1:
        context.log.warn("Multiple countries", countries=countries)

    parts = {}
    for part in location.findall("./LocationPart"):
        part_type = get_ref_value(refs, "LocPartType", part.get("LocPartTypeID"))
        parts[part_type.text] = part.findtext("./LocationPartValue/Value")

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


# def load_documents(doc):
#     documents = {}
#     for regdoc in doc.findall("./IDRegDocuments/IDRegDocument"):
#         identity_id = regdoc.get("IdentityID")
#         documents.setdefault(identity_id, [])
#         documents[identity_id].append(regdoc)
#     return documents


# def parse_registration_doc(context: Context, party, regdoc):
#     authority = regdoc.findtext("./IssuingAuthority")
#     number = regdoc.findtext("./IDRegistrationNo")
#     comment = regdoc.findtext("./Comment")
#     issue_date = None
#     expire_date = None
#     for date in regdoc.findall("./DocumentDate"):
#         period = parse_date_period(date.find("./DatePeriod"))
#         date_type_id = date.get("IDRegDocDateTypeID")
#         date_type = ref_value("IDRegDocDateType", date_type_id)
#         if date_type == "Issue Date":
#             issue_date = period
#         if date_type == "Expiration Date":
#             expire_date = period
#     country = regdoc.get("IssuedBy-CountryID")
#     if country is not None:
#         country = ref_get("Country", country).get("ISO2")
#         party.add("country", country)

#     doc_type_id = regdoc.get("IDRegDocTypeID")
#     feature = ref_value("IDRegDocType", doc_type_id)
#     if authority in ("INN", "OGRN", "IMO"):
#         feature = authority

#     h.apply_feature(
#         context,
#         party,
#         feature,
#         number,
#         country=country,
#         start_date=issue_date,
#         end_date=expire_date,
#         comment=comment,
#         authority=authority,
#     )


# def parse_party(context: Context, distinct_party, locations, documents):
#     profile = distinct_party.find("Profile")
#     sub_type = ref_get("PartySubType", profile.get("PartySubTypeID"))
#     schema = TYPES.get(sub_type.get("Value"))
#     type_ = ref_value("PartyType", sub_type.get("PartyTypeID"))
#     schema = TYPES.get(type_, schema)
#     if schema is None:
#         context.log.error("Unknown party type", value=type_)
#         return
#     party = context.make(schema)
#     party.id = context.make_slug(profile.get("ID"))
#     party.add("notes", h.clean_note(distinct_party.findtext("Comment")))
#     party.add("sourceUrl", URL % profile.get("ID"))

#     for identity in profile.findall("./Identity"):
#         # parts = {}
#         # for group in identity.findall(".//NamePartGroup"):
#         #     type_id = group.get("NamePartTypeID")
#         #     parts[group.get("ID")] = ref_value("NamePartType", type_id)

#         # for alias in identity.findall("./Alias"):
#         #     parse_alias(party, parts, alias)

#         for regdoc in documents.get(identity.get("ID"), []):
#             parse_registration_doc(context, party, regdoc)

#     for feature in profile.findall("./Feature"):
#         parse_feature(context, feature, party, locations)

#     context.emit(party, target=True)
#     # pprint(party.to_dict())
#     # context.log.info("[%s] %s" % (party.schema.name, party.caption))
#     return party


# def parse_entry(context: Context, entry, parties):
#     party_id = context.make_slug(entry.get("ProfileID"))
#     party = parties[party_id]

#     sanction = h.make_sanction(context, party, key=entry.get("ID"))
#     sanction.add("program", ref_value("List", entry.get("ListID")))

#     for event in entry.findall("./EntryEvent"):
#         date = parse_date(event.find("./Date"))
#         party.add("createdAt", date)
#         sanction.add("summary", event.findtext("./Comment"))
#         basis = ref_value("LegalBasis", event.get("LegalBasisID"))
#         sanction.add("reason", basis)

#     party.add("topics", "sanction")
#     sanction.add("listingDate", party.get("createdAt"))
#     sanction.add("startDate", party.get("modifiedAt"))

#     for measure in entry.findall("./SanctionsMeasure"):
#         sanction.add("summary", measure.findtext("./Comment"))
#         type_id = measure.get("SanctionsTypeID")
#         sanction.add("program", ref_value("SanctionsType", type_id))

#     context.emit(sanction)
#     context.emit(party, target=True)
#     # pprint(sanction.to_dict())


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
    sub_type = get_ref_value(refs, "PartySubType", sub_type_id)

    type_text = sub_type.text
    if type_text == "Unknown":
        main_type = get_ref_value(refs, "PartyType", sub_type.get("PartyTypeID"))
        type_text = main_type.text

    return TYPES[type_text]


def parse_distinct_party(
    context: Context, doc: ElementOrTree, refs: Element, party: Element
) -> None:
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

    parts: Dict[str, str] = {}
    for group in identity.findall("NamePartGroups/MasterNamePartGroup/NamePartGroup"):
        name_part = get_ref_value(refs, "NamePartType", group.get("NamePartTypeID"))
        parts[group.get("ID")] = name_part.text
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

    for feature in profile.findall("./Feature"):
        feature_id = feature.get("FeatureTypeID")
        feature_label = get_ref_value(refs, "FeatureType", feature_id).text

        version_loc = feature.find(".//VersionLocation")
        if version_loc is not None:
            location_id = version_loc.get("LocationID")
            location = doc.find(f"Locations/Location[@ID='{location_id}']")
            address = parse_location(context, refs, location)
            # TODO: handle country prop assignments
            if address.id is not None:
                context.emit(address)

            # address = locations.get(location.get("LocationID"))
            # h.apply_address(context, party, address)
            # return

        # value = None

        # period = feature.find(".//DatePeriod")
        # if period is not None:
        #     value = parse_date_period(period)
        #     h.apply_feature(context, party, feature_label, value)

        # detail = feature.find(".//VersionDetail")
        # if detail is not None:
        #     # detail_type = ref_value("DetailType", detail.get("DetailTypeID"))
        #     reference_id = detail.get("DetailReferenceID")
        #     if reference_id is not None:
        #         value = ref_value("DetailReference", reference_id)
        #     else:
        #         value = detail.text
        #     h.apply_feature(context, party, feature_label, value)
    #     pass
    # context.inspect(profile)
    # print(proxy.to_dict())


def parse_alias(
    proxy: Entity, refs: Element, parts: Dict[str, str], alias: Element
) -> None:
    # primary = as_bool(alias.get("Primary"))
    is_weak = as_bool(alias.get("LowQuality"))
    alias_type = get_ref_value(refs, "AliasType", alias.get("AliasTypeID"))
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
    doc_type = get_ref_value(refs, "IDRegDocType", doc_type_id)
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
            date_type = get_ref_value(refs, "IDRegDocDateType", date_type_id)
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
    list = get_ref_value(refs, "List", entry.get("ListID"))
    sanction.add("program", list.text)

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


# def parse_feature(
#     context: Context, proxy: Entity, refs: Element, feature: Element
# ) -> None:
#     feature_id = feature.get("FeatureTypeID")
#     feature_label = get_ref_value(refs, "FeatureType", feature_id)
#     location = feature.find(".//VersionLocation")
#     if location is not None:
#         address = locations.get(location.get("LocationID"))
#         h.apply_address(context, party, address)
#         return

#     value = None

#     period = feature.find(".//DatePeriod")
#     if period is not None:
#         value = parse_date_period(period)
#         h.apply_feature(context, party, feature_label, value)

#     detail = feature.find(".//VersionDetail")
#     if detail is not None:
#         # detail_type = ref_value("DetailType", detail.get("DetailTypeID"))
#         reference_id = detail.get("DetailReferenceID")
#         if reference_id is not None:
#             value = ref_value("DetailReference", reference_id)
#         else:
#             value = detail.text
#         h.apply_feature(context, party, feature_label, value)


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

    # context.log.info("Loading reference values...")
    # load_ref_values(doc)
    # context.log.info("Loading locations...")
    # locations = load_locations(context, doc)
    # context.log.info("Loading ID reg documents...")
    # documents = load_documents(doc)

    for distinct_party in doc.findall("./DistinctParties/DistinctParty"):
        parse_distinct_party(context, doc, refs, distinct_party)

    # parties = {}
    # for distinct_party in doc.findall(".//DistinctParty"):
    #     party = parse_party(context, distinct_party, locations, documents)
    #     parties[party.id] = party

    # # TODO: this doesn't need the parties
    # for entry in doc.findall(".//SanctionsEntry"):
    #     parse_entry(context, entry, parties)

    # for relation in doc.findall(".//ProfileRelationship"):
    #     parse_relation(context, relation, parties)
