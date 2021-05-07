from pprint import pprint  # noqa
from collections import defaultdict

from opensanctions.util import jointext, date_parts


def parse_address(node):
    if node is None:
        return {}
    address = {
        "remarks": node.findtext("./remarks"),
        "co": node.findtext("./c-o"),
        "location": node.findtext("./location"),
        "address-details": node.findtext("./address-details"),
        "p-o-box": node.findtext("./p-o-box"),
        "zip-code": node.findtext("./zip-code"),
        "area": node.findtext("./area"),
    }
    country = node.find("./country")
    if country is not None:
        address["country"] = country.get("iso-code")
    return address


def make_address(address):
    address = (
        address.get("remarks"),
        address.get("co"),
        address.get("location"),
        address.get("address-details"),
        address.get("p-o-box"),
        address.get("zip-code"),
        address.get("area"),
    )
    return jointext(*address, sep=", ")


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
            entity.add(
                "secondName", parts.get("further-given-name"), quiet=True
            )  # noqa
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
        parts = dict(place)
        parts.update(parse_address(address))
        entity.add("address", make_address(parts))
        entity.add("country", parts.get("country"))

    for bday in node.findall("./day-month-year"):
        bval = date_parts(bday.get("year"), bday.get("month"), bday.get("day"))
        entity.add("birthDate", bval)

    for nationality in node.findall("./nationality"):
        country = nationality.find("./country")
        if country is not None:
            entity.add("nationality", country.get("iso-code"))

    for bplace in node.findall("./place-of-birth"):
        place = places.get(bplace.get("place-id"))
        address = dict(place)
        address.update(parse_address(bplace))
        entity.add("birthPlace", make_address(address))
        entity.add("country", address.get("country"))

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
        passport.make_id(entity.id, "Passport", doc.get("ssid"))
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
        # node = target.find('./object')
        # TODO: build out support for these!
        return

    dates = set()
    for mod in target.findall("./modification"):
        date = mod.get("publication-date")
        if date is not None:
            dates.add(date)
    if not len(dates):
        dates.add(updated_at)
    entity.context["created_at"] = min(dates)
    entity.context["updated_at"] = max(dates)

    entity.make_slug(target.get("ssid"))
    for other in node.findall("./other-information"):
        entity.add("notes", other.text)

    sanction = context.make("Sanction")
    sanction.make_id(entity.id, "Sanction")
    sanction.add("entity", entity)
    sanction.add("authority", "Swiss SECO Consolidated Sanctions")
    sanction.add("sourceUrl", context.dataset.url)
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
    context.fetch_artifact("source.xml", context.dataset.data.url)
    doc = context.parse_artifact_xml("source.xml")
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
