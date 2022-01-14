from collections import defaultdict
from prefixdate import parse_parts

from opensanctions.core import Context
from opensanctions import helpers as h
from opensanctions.util import jointext

# TODO: sanctions-program full parse


def parse_address(node):
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


def compose_address(context: Context, entity, place, el):
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


def whole_name(parts):
    name = (
        parts.get("given-name"),
        parts.get("further-given-name"),
        parts.get("father-name"),
        parts.get("family-name"),
        parts.get("grand-father-name"),
        parts.get("tribal-name"),
        parts.get("whole-name"),
        parts.get("other"),
    )
    return jointext(*name, sep=" ")


def parse_name(entity, node):
    name_type = node.get("name-type")
    quality = node.get("quality")
    parts = defaultdict(dict)
    for part in node.findall("./name-part"):
        part_type = part.get("name-part-type")
        value = part.findtext("./value")
        parts[None][part_type] = value

        for spelling in part.findall("./spelling-variant"):
            key = (spelling.get("lang"), spelling.get("script"))
            parts[key][part_type] = spelling.text

    for key, parts in parts.items():
        name = whole_name(parts)
        if quality != "low":
            # TODO: suffix
            entity.add("title", parts.get("title"), quiet=True)
            entity.add("firstName", parts.get("given-name"), quiet=True)
            entity.add("secondName", parts.get("further-given-name"), quiet=True)
            entity.add("lastName", parts.get("family-name"), quiet=True)
            entity.add("lastName", parts.get("maiden-name"), quiet=True)
            entity.add("fatherName", parts.get("father-name"), quiet=True)
            if name_type == "primary-name" and key is None:
                entity.add("name", name)
            else:
                entity.add("alias", name)
        else:
            entity.add("weakAlias", name)


async def parse_identity(context: Context, entity, node, places):
    for name in node.findall(".//name"):
        parse_name(entity, name)

    for address in node.findall(".//address"):
        place = places.get(address.get("place-id"))
        address = compose_address(context, entity, place, address)
        await h.apply_address(context, entity, address)

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
        country = doc.find("./issuer")
        type_ = doc.get("document-type")
        number = doc.findtext("./number")
        entity.add("nationality", country.text, quiet=True)
        schema = "Identification"
        if type_ in ("id-card"):
            entity.add("idNumber", number)
        if type_ in ("passport", "diplomatic-passport"):
            entity.add("idNumber", number)
            schema = "Passport"
        passport = context.make(schema)
        passport.id = context.make_id(entity.id, type_, doc.get("ssid"))
        passport.add("holder", entity)
        passport.add("country", country.text)
        passport.add("number", number)
        passport.add("type", type_)
        passport.add("summary", doc.findtext("./remark"))
        passport.add("startDate", doc.findtext("./date-of-issue"))
        passport.add("endDate", doc.findtext("./expiry-date"))
        await context.emit(passport)


async def parse_entry(context: Context, target, programs, places, updated_at):
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

    dates = set()
    for mod in target.findall("./modification"):
        date = mod.get("publication-date")
        if date is not None:
            dates.add(date)
    if not len(dates):
        dates.add(updated_at)
    entity.context["created_at"] = min(dates)
    entity.context["updated_at"] = max(dates)

    entity.id = context.make_slug(target.get("ssid"))
    entity.add("gender", node.get("sex"), quiet=True)
    for other in node.findall("./other-information"):
        value = other.text.strip()
        if entity.schema.is_a("Vessel") and value.lower().startswith("imo"):
            _, imo_num = value.split(":", 1)
            entity.add("imoNumber", imo_num)
        else:
            entity.add("notes", value)

    sanction = h.make_sanction(context, entity)
    sanction.add("modifiedAt", max(dates))

    ssid = target.get("sanctions-set-id")
    sanction.add("program", programs.get(ssid))

    for justification in node.findall("./justification"):
        # TODO: should this go into sanction:reason?
        entity.add("notes", justification.text)

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
        # await context.emit(rel_target)

        entity.add_schema(rel.schema.get(res.source).range)
        await context.emit(rel)

    for identity in node.findall("./identity"):
        await parse_identity(context, entity, identity, places)

    entity.add("topics", "sanction")
    await context.emit(entity, target=True)
    await context.emit(sanction)


async def crawl(context: Context):
    path = await context.fetch_resource("source.xml", context.dataset.data.url)
    await context.export_resource(path, "text/xml", title=context.SOURCE_TITLE)
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
        await parse_entry(context, target, programs, places, updated_at)
