import re
from itertools import product
from datetime import datetime
from prefixdate import parse_parts
from typing import Dict, Optional, Tuple, List, Pattern
from followthemoney.types import registry
from followthemoney.util import join_text
from lxml.etree import _Element as Element
from normality import slugify

from zavod import Context, Entity
from zavod import helpers as h

# TODO: sanctions-program full parse
MayStr = Optional[str]

NAME_QUALITY_WEAK: Dict[MayStr, bool] = {"good": False, "low": True}
NAME_TYPE: Dict[MayStr, str] = {
    "primary-name": "name",
    "alias": "alias",
    "formerly-known-as": "previousName",
}
NAME_PARTS: Dict[MayStr, MayStr] = {
    "title": "title",
    "given-name": "firstName",
    "further-given-name": "secondName",
    "father-name": "fatherName",
    "grand-father-name": "fatherName",
    "family-name": "lastName",
    "maiden-name": "lastName",
    "suffix": "nameSuffix",
    "tribal-name": "weakAlias",
    "whole-name": None,
    "other": None,
}


OTHER_INFO_DEFINITIONS = [
    # Website related
    r"(?P<whole>(?P<key>Website:) (?P<value>(https?:\/\/|www\.)\S+))",
    r"(?P<whole>(?P<key>Website:)\s*(?P<value>.+))"
    r"(?P<whole>(?P<key>Company website:)\s*(?P<value>.+))"
    # Email related
    r"(?P<whole>(?P<key>E-?mail(?: address)?\s*:) (?P<value>[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}))",
    r"(?P<whole>(?P<key>Company email:)\s*(?P<value>.+))",
    # Phone/Fax related
    r"(?P<whole>(?P<key>(Tel\.|Telephone)( number)? ?:| Tel ?:| Tel.:|Phone:|Phone number:)\s*(?P<value>(?:\+?[0-9\- ()]+)(?:,\s*\+?[0-9\- ()]+)*))",
    r"(?P<whole>(?P<key>Telephone no\.:)\s*(?P<value>(?:[\+\d\s-]+)(?:[,;]\s*[\+\d\s-]+)*))",
    r"(?P<whole>(?P<key>Tel:)\s*(?P<value>(?:\+?[0-9\s-]+)(?:[,;]\s*\+?[0-9\s-]+)*))",
    r"(?P<whole>(?P<key>Company phone:)\s*(?P<value>.+))",
    r"(?P<whole>(?P<key>(Fax) ?:) (?P<value>\+?[0-9- ()]+))",
    r"(?P<whole>(?P<key>Fax no\.:)\s*(?P<value>\+?[0-9\s-]+))",
    # Identification numbers
    r"(?P<whole>(?P<key>(Taxpayer [Ii]dentification [Nn]umber) ?:) (?P<value>\d+)\.?)",
    r"(?P<whole>(?P<key>(ОГРН/main )?([Ss]tate |Business )?[Rr]egistration number ?:) (?P<value>\d+)\.?)",
    r"(?P<whole>(?P<key>(Tax [Rr]egistration [Nn]umber) ?:) (?P<value>\d+)\.?)",
    r"(?P<whole>(?P<key>(Tax [Ii]dentification [Nn]umber) ?:|Tax ID number:|Tax ID No.) (?P<value>\d+)\.?)",
    r"(?P<whole>(?P<key>National [Ii]dentification [Nn]umber ?:| National ID number:) (?P<value>\d+)) ?\(passport\)?",
    r"(?P<whole>(?P<key>State [Ii]dentification [Nn]umber(?: \([A-Z]+\))? ?:)\s*(?P<value>\d+)\s*(?P<extra>\([^)]+\))?)",
    r"(?P<whole>(?P<key>(Passport number) ?:) (?P<value>[A-Z]{2}\d{7}))",
    r"(?P<whole>(?P<key>National ID\.:)\s*(?P<value>\d+))",
    r"(?P<whole>(?P<key>INN:)\s*(?P<value>\d+))",
    r"(?P<whole>(?P<key>OKPO:)\s*(?P<value>\d+))",
    r"(?P<whole>(?P<key>UNP:)\s*(?P<value>\d+))",
    r"^(?P<whole>(?P<key>Registration number:)\s*(?P<value>\d+))$",
    r"(?P<whole>(?P<key>Registration number \(OGRN\):)\s*(?P<value>\d+))",
    r"(?P<whole>(?P<key>Registration number \(SNR\):)\s*(?P<value>.+))",
    r"(?P<whole>(?P<key>(ID number) ?:|ID Card Number:) (?P<value>[A-Za-z0-9]+)\.?)",
    r"(?P<whole>(?P<key>Economic code:)\s*(?P<value>\d+))",
    r"(?P<whole>(?P<key>KPP ?:)\s*(?P<value>\d+))",
    r"(?P<whole>(?P<key>OGRN ?:)\s*(?P<value>\d+))",
    r"(?P<whole>(?P<key>Syrian National ID Number:)\s*(?P<value>\d+))",
    r"(?P<whole>(?P<key>(IMO [Nn]umber) ?:) (?P<value>\d+)\.?)",
    r"(?P<whole>(?P<key>Wagner Group ID:)\s*(?P<value>.+))",
    # Place and Location related
    r"(?P<whole>(?P<key>Place of registration ?:)\s*(?P<value>.+))",
    r"(?P<whole>(?P<key>Principal place of business ?:)\s*(?P<value>.+))",
    r"(?P<whole>(?P<key>Suspected location:)\s*(?P<value>.+))",
    r"(?P<whole>(?P<key>Nationality:)\s*(?P<value>[\w\s]+ until \d{4}\.))",
    r"(?P<whole>(?P<key>Address:)\s*(?P<value>[\w\s]+ in [\w\s]+\.))",
    # Date related
    r"(?P<whole>(?P<key>Date range:)\s*(?P<value>DOB between (?:\d{4}–\d{4}|\d{4}\s*and\s*\d{4})(?:\.\s*Date range: DOB between (?:\d{4}–\d{4}|\d{4}\s*and\s*\d{4}))*\.?))",
    r"(?P<whole>(?P<key>Date of registration:)\s*(?P<value>\d{1,2} [A-Za-z]+ \d{4}))",
    r"(?P<whole>(?P<key>Date of registration:)\s*(?P<value>.+))",
    r"(?P<whole>(?P<key>DOB:)\s*(?P<value>.+))",
    # Role and Title related
    r"(?P<whole>(?P<key>(Function) ?:) (?P<value>.+))",
    r"(?P<whole>(?P<key>(Title) ?:) (?P<value>.+))",
    r"(?P<whole>(?P<key>(Rank) ?:) (?P<value>.+))",
    r"(?P<whole>(?P<key>(Position\(s\)) ?:|) (?P<value>.+))",
    r"(?P<whole>(?P<key>(Position) ?:) (?P<value>.+))",
    r"(?P<whole>(?P<key>(Designation) ?:) (?P<value>.+))",
    r"(?P<whole>(?P<key>Owner and chairman ?:)\s*(?P<value>.+))",
    r"(?P<whole>(?P<key>Active region ?:)\s*(?P<value>.+))",
    r"(?P<whole>(?P<key>(Type of entity) ?:) (?P<value>.+))"
    # Others
    r"(?P<whole>(?P<key>Associated entities:)\s*(?P<value>[^;]+(?:; [^;]+)*))",
    r"(?P<whole>(?P<key>Associated individuals:)\s*(?P<value>.+))",
    r"(?P<whole>(?P<key>Other identifying information:)\s*(?P<value>.+))",
    r"(?P<whole>(?P<key>Associated entities and individuals:)\s*(?P<value>.+))",
    r"(?P<whole>(?P<key>Other associated entities:)\s*(?P<value>.+))"
    r"(?P<whole>(?P<key>Identity document number:)\s*(?P<value>.+))",
    r"(?P<whole>(?P<key>Personal ID:)\s*(?P<value>.+))",
    r"(?P<whole>(?P<key>Tax payer ID:)\s*(?P<value>\d+))",
    r"(?P<whole>(?P<key>Status:)\s*(?P<value>.+))",
    r"(?P<whole>(?P<key>Callsign:)\s*(?P<value>[\w\s]+))",
]
# [Old reference # E.29.II.3]
#

OTHER_INFO_REGEXES: List[Tuple[Pattern, str]] = [
    (re.compile(pattern), name) for pattern, name in OTHER_INFO_DEFINITIONS
]


def process_entry(value, regex_patterns):
    for regex, name in regex_patterns:
        match = regex.match(value)
        if match:
            return {
                "regex": regex.pattern,
                "whole": match.group("whole"),
                "key": match.group("key"),
                "value": match.group("value"),
            }
    return None


def parse_address(node: Element):
    address = {
        "remarks": node.findtext("./remarks"),
        "co": node.findtext("./c-o"),
        "location": node.findtext("./location"),
        "address-details": node.findtext("./address-details"),
        "p-o-box": node.findtext("./p-o-box"),
        "zip-code": node.findtext("./zip-code"),
        "area": node.findtext("./area"),
        "country": node.findtext("./country"),
    }
    return {k: v for (k, v) in address.items() if v is not None}


def compose_address(
    context: Context, entity: Entity, place, el: Element, country_prop: str = "country"
) -> Optional[Entity]:
    addr = dict(place)
    addr.update(parse_address(el))
    entity.add(country_prop, addr.get("country"))
    po_box = addr.get("p-o-box")
    if po_box is not None:
        po_box = f"P.O. Box {po_box}"
    return h.make_address(
        context,
        remarks=addr.get("remarks"),
        summary=addr.get("co"),
        street=addr.get("address-details"),
        city=addr.get("location"),
        po_box=po_box,
        postal_code=addr.get("zip-code"),
        region=addr.get("area"),
        country=addr.get("country"),
    )


def parse_name(context: Context, entity: Entity, node: Element):
    # verification:
    # al-Nu'Aymi   - in full name
    # Lutsky   - Lutsky Ihar Uladzimiravich as primary name

    name_prop = NAME_TYPE[node.get("name-type")]
    is_weak = NAME_QUALITY_WEAK[node.get("quality")]
    if is_weak:
        name_prop = "weakAlias"

    max_order: int = 0
    parts: List[Tuple[str, MayStr, MayStr, int, str]] = []
    for part_node in node.findall("./name-part"):
        part_type = part_node.get("name-part-type")
        order_str = part_node.get("order")
        assert order_str is not None and part_type is not None
        order = int(order_str)
        max_order = max(order, max_order)
        value = part_node.findtext("./value")
        if value is None:
            continue
        parts.append((part_type, None, None, order, value))

        for spelling in part_node.findall("./spelling-variant"):
            lang = registry.language.clean(spelling.get("lang"))
            script = spelling.get("script")
            if spelling.text is None:
                continue
            parts.append((part_type, lang, script, order, spelling.text))

    ordered: Dict[Tuple[MayStr, MayStr], Dict[int, List[MayStr]]] = {}
    for part_type, lang, script, order, value in parts:
        # if part_type in ("suffix", "title"):
        #     print("XXX", part_type, value)

        # Begin building whole names:
        cult = (lang, script)
        if cult not in ordered:
            ordered[cult] = {}
        if order not in ordered[cult]:
            ordered[cult][order] = []
        if part_type == "title":
            ordered[cult][order].append(None)
        else:
            ordered[cult][order].append(value)

        if part_type not in NAME_PARTS:
            context.log.warn("Unknown name part", part_type=part_type)
            continue
        part_prop = NAME_PARTS[part_type]
        if part_type == "whole-name":
            part_prop = name_prop
        if part_prop == "name" and lang is not None:
            part_prop = "alias"
        if part_prop is not None:
            entity.add(part_prop, value, lang=lang, quiet=True)

    for (lang, script), ords in ordered.items():
        whole_parts: List[List[MayStr]] = []
        for order in range(1, max_order + 1):
            values = ords.get(order, ordered[(None, None)][order])
            whole_parts.append(values)

        for prod in product(*whole_parts):
            whole_name = join_text(*prod)
            full_prop = name_prop
            if full_prop == "name" and lang is not None:
                full_prop = "alias"
            entity.add(full_prop, whole_name, lang=lang)


def parse_identity(context: Context, entity: Entity, node: Element, places):
    for name in node.findall(".//name"):
        parse_name(context, entity, name)

    for address_node in node.findall(".//address"):
        place = places.get(address_node.get("place-id"))
        address = compose_address(context, entity, place, address_node)
        h.apply_address(context, entity, address)

    for bday in node.findall(".//day-month-year"):
        bval = parse_parts(bday.get("year"), bday.get("month"), bday.get("day"))
        if entity.schema.is_a("Person"):
            entity.add("birthDate", bval)
        else:
            entity.add("incorporationDate", bval)

    for nationality in node.findall(".//nationality"):
        country = nationality.find("./country")
        if country is not None:
            entity.add("nationality", country.get("iso-code"))
            entity.add("nationality", country.text)

    for bplace in node.findall(".//place-of-birth"):
        place = places.get(bplace.get("place-id"))
        address = compose_address(
            context, entity, place, bplace, country_prop="birthCountry"
        )
        if address is not None:
            entity.add("birthPlace", address.get("full"))

    for doc in node.findall(".//identification-document"):
        type_ = doc.get("document-type")
        is_passport = type_ in ("passport", "diplomatic-passport")
        doc_country = doc.findtext("./issuer")
        entity.add("nationality", doc_country, quiet=True)
        passport = h.make_identification(
            context,
            entity,
            number=doc.findtext("./number"),
            doc_type=type_,
            country=doc_country,
            summary=doc.findtext("./remark"),
            start_date=doc.findtext("./date-of-issue"),
            end_date=doc.findtext("./expiry-date"),
            key=doc.get("ssid"),
            passport=is_passport,
        )
        if passport is not None:
            context.emit(passport)


def parse_entry(context: Context, target: Element, programs, places):
    entity = context.make("LegalEntity")
    node = target.find("./entity")
    if node is None:
        node = target.find("./individual")
        entity = context.make("Person")
    if node is None:
        node = target.find("./object")
        if node is None:
            context.log.error("No target", target=target)
            return
        object_type = node.get("object-type")
        if object_type != "vessel":
            context.log.warning(
                "Unknown target type", target=target, object_type=object_type
            )
        entity = context.make("Vessel")

    entity_ssid = target.get("ssid")
    entity.id = context.make_slug(entity_ssid)
    entity.add("gender", node.get("sex"), quiet=True)
    for other in node.findall("./other-information"):
        if other.text is None:
            continue
        value = other.text.strip()
        # Add auto-parsed properties
        result = process_entry(value, OTHER_INFO_REGEXES)
        if result:
            # context.log.info("Match found", value=value, match=result)
            print("Original Value %r:" % value)
            print(f"Match: {result}")
            print(f"Key: {result['key']}")
            print(f"Slugified Key: {slugify(result['key'])}")
            print(f"Value: {result['value']}")

            prop = context.lookup_value("properties", slugify(result["key"]))
            if prop is not None:
                if prop != "imoNumber" and prop != "incorporationDate":
                    entity.add(prop, result["value"])
                elif prop == "incorporationDate":
                    h.apply_date(entity, prop, result["value"])
                elif prop == "imoNumber" and entity.schema.name == "Vessel":
                    entity.add(prop, result["value"])
            else:
                context.log.warning(
                    "Unrecognized property",
                    key=result["key"],
                    slugified_key=slugify(result["key"]),
                )

            # # Remove matched part from original value
            # pattern = re.escape(
            #     result["whole"]
            # )  # Escape any special characters in result["whole"]
            # value_before = value
            # value = re.sub(
            #     pattern, "", value
            # ).strip()  # Strip any leading/trailing whitespace

            # print(
            #     f"Value before: {value_before}, value after: {value}",
            # )
            value = value.replace(result["whole"], "").strip()
            # print(f"Remaining Value: {value}")
            if not value:
                continue
        # context.log.warning("Unprocessed remaining value", value=value)

        res = context.lookup("other_information", value)
        if not res:
            context.log.warning("More information available", value=value)
            continue
        if not (res.properties or res.relations):
            context.log.warning("No properties or relations", value=value)
            continue
        # Add artisinal properties
        if res.properties:
            for item in res.properties:
                entity.add(item["prop"], item["value"])
        # Create relations and related entities
        if res.relations:
            for rel in res.relations:
                other_entity = context.make(rel["other_schema"])
                other_entity.id = context.make_id(rel["other_name"], entity.id)
                other_entity.add("name", rel["other_name"])
                rel_entity = context.make(rel["rel_schema"])
                rel_entity.id = context.make_slug(entity.id, other_entity.id)
                rel_entity.add(rel["this_prop"], entity.id)
                rel_entity.add(rel["other_prop"], other_entity.id)
                context.emit(rel_entity)
                context.emit(other_entity)

    sanction = h.make_sanction(context, entity)
    sanction.add("authorityId", entity_ssid)
    sanctioned = True
    dates = set()
    for mod in target.findall("./modification"):
        mod_type = mod.get("modification-type")
        effective_date = mod.get("effective-date")
        dates.add(mod.get("publication-date"))
        if mod_type == "de-listed":
            sanction.add("endDate", effective_date)
            sanctioned = False
            continue
        sanction.add("listingDate", mod.get("publication-date"))
        sanction.add("startDate", effective_date)
    dates_ = [d for d in dates if d is not None]
    if len(dates_):
        entity.add("createdAt", min(dates_))
        entity.add("modifiedAt", max(dates_))

    ssid = target.get("sanctions-set-id")
    if ssid is None:
        ssid = target.findtext("./sanctions-set-id")
    sanction.add("program", programs.get(ssid))
    foreign_id = target.findtext("./foreign-identifier")
    sanction.add("unscId", foreign_id)

    justifications: List[Tuple[str, str]] = []
    for justification in node.findall("./justification"):
        ssid = justification.get("ssid")
        justifications.append((ssid, justification.text))

    # TODO: should this go into sanction:reason?
    notes = [n for (s, n) in sorted(justifications)]
    entity.add("notes", h.clean_note("\n\n".join(notes)))

    for relation in node.findall("./relation"):
        rel_type = relation.get("relation-type")
        target_id = context.make_slug(relation.get("target-id"))
        res = context.lookup("relations", rel_type)
        if res is None:
            context.log.warn(
                "Unknown relationship type",
                type=rel_type,
                source=entity,
                target=target_id,
            )
            continue

        rel = context.make(res.schema)
        rel.id = context.make_slug(relation.get("ssid"), rel_type)
        rel.add(res.source, entity.id)
        rel.add(res.target, target_id)
        rel.add(res.text, rel_type)

        # rel_target = context.make(rel.schema.get(res.target).range)
        # rel_target.id = target_id
        # context.emit(rel_target)
        source_prop = rel.schema.get(res.source)
        if source_prop is not None and source_prop.range is not None:
            entity.add_schema(source_prop.range)
        context.emit(rel)

    for identity in node.findall("./identity"):
        parse_identity(context, entity, identity, places)

    if sanctioned:
        entity.add("topics", "sanction")
    context.emit(entity, target=sanctioned)
    context.emit(sanction)


def crawl(context: Context):
    path = context.fetch_resource("source.xml", context.data_url)
    context.export_resource(path, "text/xml", title=context.SOURCE_TITLE)
    doc = context.parse_resource_xml(path)
    root = doc.getroot()
    assert root is not None
    date = root.get("date")
    if date is not None:
        context.data_time = datetime.strptime(date, "%Y-%m-%d")

    programs: Dict[str, MayStr] = {}
    for sanc in doc.findall(".//sanctions-program"):
        sanc_set = sanc.find("./sanctions-set")
        if sanc_set is None:
            context.log.warning("No sanctions-set", program=sanc)
            continue
        ssid = sanc_set.get("ssid")
        if ssid is None:
            continue
        programs[ssid] = sanc.findtext('./program-name[@lang="eng"]')

    places = {}
    for place in doc.findall(".//place"):
        places[place.get("ssid")] = parse_address(place)

    for target in doc.findall("./target"):
        parse_entry(context, target, programs, places)
