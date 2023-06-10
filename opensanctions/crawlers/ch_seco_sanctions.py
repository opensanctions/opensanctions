import re
from prefixdate import parse_parts
from collections import defaultdict
from typing import Dict, Optional, Tuple
from followthemoney.types import registry
from followthemoney.util import join_text
from lxml.etree import _Element as Element

from opensanctions.core import Context, Entity
from opensanctions import helpers as h

# TODO: sanctions-program full parse
MayStr = Optional[str]

NAME_QUALITY_WEAK = {"good": False, "low": True}
NAME_TYPE = {
    "primary-name": "name",
    "alias": "alias",
    "formerly-known-as": "previousName",
}
# Some metadata is dirty text in <other-information> tags
# TODO: take in charge multiple values
REGEX_WEBSITE = re.compile("Website ?: ((https?:|www\.)\S*)")
REGEX_EMAIL = re.compile(
    "E-?mail( address)? ?: ([A-Za-z0-9._-]+@[A-Za-z0-9-]+(\.[A-Z|a-z]{2,})+)"
)
REGEX_PHONE = re.compile("(Tel\.|Telephone)( number)? ?: (\+?[0-9- ()]+)")
REGEX_INN = re.compile("Taxpayer [Ii]dentification [Nn]umber ?: (\d+)\.?")
REGEX_REGNUM = re.compile(
    "(ОГРН/main )?([Ss]tate |Business )?[Rr]egistration number ?: (\d+)\.?"
)
REGEX_TAX = re.compile("Tax [Rr]egistration [Nn]umber ?: (\d+)\.?")
REGEX_IMO = re.compile("IMO [Nn]umber ?: (\d+)\.?")
FORMATS = ["%d.%m.%Y", "%Y", "%b %Y", "%d %B %Y", "%d %b %Y", "%b, %Y"]


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


def compose_address(context: Context, entity: Entity, place, el: Element):
    addr = dict(place)
    addr.update(parse_address(el))
    entity.add("country", addr.get("country"))
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


def parse_name(entity: Entity, node: Element):
    name_prop = NAME_TYPE[node.get("name-type")]
    is_weak = NAME_QUALITY_WEAK[node.get("quality")]

    parts: Dict[Tuple[MayStr, MayStr], Dict[MayStr, MayStr]] = defaultdict(dict)
    for part_node in node.findall("./name-part"):
        part_type = part_node.get("name-part-type")
        value = part_node.findtext("./value")
        parts[(None, None)][part_type] = value

        for spelling in part_node.findall("./spelling-variant"):
            key = (spelling.get("lang"), spelling.get("script"))
            parts[key][part_type] = spelling.text

    for (lang, _), part in parts.items():
        if name_prop == "name":
            name_prop = name_prop if lang is None else "alias"
        lang = registry.language.clean(lang)
        entity.add("title", part.pop("title", None), quiet=True, lang=lang)
        suffix = part.pop("suffix", None)
        entity.add("title", suffix, quiet=True, lang=lang)
        entity.add("weakAlias", part.pop("other", None), quiet=True, lang=lang)
        entity.add("weakAlias", part.pop("tribal-name", None), quiet=True, lang=lang)
        grand_father_name = part.pop("grand-father-name", None)
        entity.add("fatherName", grand_father_name, quiet=True, lang=lang)

        # SECO shows grand father name as suffix
        full_suffix = join_text(grand_father_name, suffix)

        h.apply_name(
            entity,
            full=part.pop("whole-name", None),
            given_name=part.pop("given-name", None),
            second_name=part.pop("further-given-name", None),
            patronymic=part.pop("father-name", None),
            last_name=part.pop("family-name", None),
            maiden_name=part.pop("maiden-name", None),
            suffix=full_suffix,
            is_weak=is_weak,
            name_prop=name_prop,
            quiet=True,
            lang=lang,
        )
        h.audit_data(part)


def parse_identity(context: Context, entity: Entity, node: Element, places):
    for name in node.findall(".//name"):
        parse_name(entity, name)

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
        address = compose_address(context, entity, place, bplace)
        entity.add("birthPlace", address.get("full"))

    for doc in node.findall(".//identification-document"):
        type_ = doc.get("document-type")
        is_passport = type_ in ("passport", "diplomatic-passport")
        country = doc.find("./issuer")
        entity.add("nationality", country.text, quiet=True)
        passport = h.make_identification(
            context,
            entity,
            number=doc.findtext("./number"),
            doc_type=type_,
            country=country.text,
            summary=doc.findtext("./remark"),
            start_date=doc.findtext("./date-of-issue"),
            end_date=doc.findtext("./expiry-date"),
            key=doc.get("ssid"),
            passport=is_passport,
        )
        if passport is not None:
            context.emit(passport)


def parse_entry(context: Context, target, programs, places, updated_at):
    entity = context.make("LegalEntity")
    node = target.find("./entity")
    if node is None:
        node = target.find("./individual")
        entity = context.make("Person")
    if node is None:
        node = target.find("./object")
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
        value = other.text.strip()
        imo_num = REGEX_IMO.fullmatch(value)
        reg_num = REGEX_REGNUM.fullmatch(value)
        inn_match = REGEX_INN.fullmatch(value)
        if entity.schema.is_a("Vessel") and imo_num:
            entity.add("imoNumber", imo_num.group(1))
        elif entity.schema.is_a("LegalEntity") and value.startswith(
            "Date of registration"
        ):
            _, reg_date = value.split(":", 1)
            reg_date = reg_date.strip()
            entity.add("incorporationDate", h.parse_date(reg_date, FORMATS))
        elif entity.schema.is_a("LegalEntity") and value.startswith("Type of entity"):
            _, legalform = value.split(":", 1)
            entity.add("legalForm", legalform)
        elif entity.schema.is_a("LegalEntity") and reg_num:
            entity.add("registrationNumber", reg_num.group(3))
        elif inn_match:
            entity.add("innCode", inn_match.group(1))
        elif tax := REGEX_TAX.fullmatch(value):
            entity.add("taxNumber", tax.group(1))
        elif website := REGEX_WEBSITE.fullmatch(value):
            entity.add("website", website.group(1))
        elif email := REGEX_EMAIL.fullmatch(value):
            entity.add("email", email.group(2))
        elif phonenumber := REGEX_PHONE.fullmatch(value):
            entity.add("phone", phonenumber.group(3))
        elif value == "Registration number: ИНН":
            pass
        else:
            entity.add("notes", h.clean_note(value))

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
    sanction.add("program", programs.get(ssid))

    for justification in node.findall("./justification"):
        # TODO: should this go into sanction:reason?
        entity.add("notes", h.clean_note(justification.text))

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
        rel.id = context.make_slug(relation.get("ssid"))
        rel.add(res.source, entity.id)
        rel.add(res.target, target_id)
        rel.add(res.text, rel_type)

        # rel_target = context.make(rel.schema.get(res.target).range)
        # rel_target.id = target_id
        # context.emit(rel_target)

        entity.add_schema(rel.schema.get(res.source).range)
        context.emit(rel)

    for identity in node.findall("./identity"):
        parse_identity(context, entity, identity, places)

    if sanctioned:
        entity.add("topics", "sanction")
    context.emit(entity, target=sanctioned)
    context.emit(sanction)


def crawl(context: Context):
    path = context.fetch_resource("source.xml", context.source.data.url)
    context.export_resource(path, "text/xml", title=context.SOURCE_TITLE)
    doc = context.parse_resource_xml(path)
    updated_at = doc.getroot().get("date")

    programs = {}
    for sanc in doc.findall(".//sanctions-program"):
        ssid = sanc.find("./sanctions-set").get("ssid")
        programs[ssid] = sanc.findtext('./program-name[@lang="eng"]')

    places = {}
    for place in doc.findall(".//place"):
        places[place.get("ssid")] = parse_address(place)

    for target in doc.findall("./target"):
        parse_entry(context, target, programs, places, updated_at)
