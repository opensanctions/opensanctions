from collections import defaultdict
from prefixdate import parse_parts

from opensanctions.helpers import make_address, apply_address, make_sanction
from opensanctions.util import jointext


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


def compose_address(context, entity, place, el):
    addr = dict(place)
    addr.update(parse_address(el))
    entity.add("country", addr.get("country"))
    po_box = addr.get("p-o-box")
    if po_box is not None:
        po_box = f"P.O. Box {po_box}"
    return make_address(
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


def parse_identity(context, entity, node, places):
    for name in node.findall("./name"):
        parse_name(entity, name)

    for address in node.findall("./address"):
        place = places.get(address.get("place-id"))
        address = compose_address(context, entity, place, address)
        apply_address(context, entity, address)

    for bday in node.findall("./day-month-year"):
        bval = parse_parts(bday.get("year"), bday.get("month"), bday.get("day"))
        entity.add("birthDate", bval)

    for nationality in node.findall("./nationality"):
        country = nationality.find("./country")
        if country is not None:
            entity.add("nationality", country.get("iso-code"))

    for bplace in node.findall("./place-of-birth"):
        place = places.get(bplace.get("place-id"))
        address = compose_address(context, entity, place, bplace)
        entity.add("birthPlace", address.get("full"))

    for doc in node.findall("./identification-document"):
        country = doc.find("./issuer")
        type_ = doc.get("document-type")
        number = doc.findtext("./number")
        entity.add("nationality", country.get("code"), quiet=True)
        if type_ in ["id-card"]:
            entity.add("idNumber", number)
        if type_ in ["passport", "diplomatic-passport"]:
            entity.add("idNumber", number)
        passport = context.make("Passport")
        passport.id = context.make_id(entity.id, "Passport", doc.get("ssid"))
        passport.add("holder", entity)
        passport.add("country", country.get("code"))
        passport.add("passportNumber", number)
        passport.add("type", type_)
        passport.add("summary", doc.findtext("./remark"))
        context.emit(passport)


def parse_entry(context, target, programs, places, updated_at):
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
    for other in node.findall("./other-information"):
        value = other.text.strip()
        if entity.schema.is_a("Vessel") and value.lower().startswith("imo"):
            _, imo_num = value.split(":", 1)
            entity.add("imoNumber", imo_num)
        else:
            entity.add("notes", value)

    sanction = make_sanction(context, entity)
    sanction.add("modifiedAt", max(dates))

    for justification in node.findall("./justification"):
        sanction.add("reason", justification.text)

    ssid = target.get("sanctions-set-id")
    sanction.add("program", programs.get(ssid))

    for identity in node.findall("./identity"):
        parse_identity(context, entity, identity, places)

    context.emit(entity, target=True, unique=True)
    context.emit(sanction)


def crawl(context):
    path = context.fetch_resource("source.xml", context.dataset.data.url)
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
