from pprint import pprint  # noqa

from opensanctions import constants
from ftmstore.memorious import EntityEmitter
from opensanctions.util import jointext

GENDERS = {"M": constants.MALE, "F": constants.FEMALE}

NS = {"default": "http://eu.europa.ec/fpi/fsd/export"}


def parse_entry(emitter, entry):
    entity = emitter.make("LegalEntity")
    if entry.find("./default:subjectType", NS).get("classificationCode") == "P":
        entity = emitter.make("Person")
    entity.make_id(entry.get("logicalId"))
    entity.add(
        "sourceUrl",
        entry.findtext("./default:regulation/default:publicationUrl", "", NS),
    )

    sanction = emitter.make("Sanction")
    sanction.make_id(entity.id)
    sanction.add("entity", entity)
    sanction.add("authority", "European Union")
    sanction.add(
        "sourceUrl",
        entry.findtext("./default:regulation/default:publicationUrl", "", NS),
    )
    program = jointext(
        entry.find("./default:regulation", NS).get("programme"),
        entry.find("./default:regulation", NS).get("numberTitle"),
        sep=" - ",
    )
    sanction.add("program", program)
    sanction.add("reason", entry.findtext("./default:remark", "", NS))
    sanction.add(
        "startDate", entry.find("./default:regulation", NS).get("entryIntoForceDate")
    )

    for name in entry.findall("./default:nameAlias", NS):
        if entity.has("name"):
            entity.add("alias", name.get("wholeName"))
        else:
            entity.add("name", name.get("wholeName"))
        entity.add("title", name.get("title"), quiet=True)
        entity.add("firstName", name.get("firstName"), quiet=True)
        entity.add("middleName", name.get("middleName"), quiet=True)
        entity.add("lastName", name.get("lastName"), quiet=True)
        entity.add("position", name.get("function"), quiet=True)
        gender = GENDERS.get(name.get("gender"))
        entity.add("gender", gender, quiet=True)

    for pnode in entry.findall(
        './default:identification[@identificationTypeCode="passport"]', NS
    ):
        passport = emitter.make("Passport")
        passport.make_id("Passport", entity.id, pnode.get("number"))
        passport.add("holder", entity)
        passport.add("passportNumber", pnode.get("number"))
        passport.add("country", pnode.get("countryIso2Code"))
        emitter.emit(passport)

    for node in entry.findall("./default:address", NS):
        address = jointext(
            node.get("street"),
            node.get("city"),
            node.findtext("default:zipCode", "", NS),
        )
        entity.add("address", address)
        entity.add("country", node.get("countryIso2Code"))

    for birth in entry.findall("./default:birthdate", NS):
        entity.add("birthDate", birth.get("year"))

    for country in entry.findall("./default:citizenship", NS):
        entity.add("nationality", country.get("countryIso2Code"), quiet=True)

    emitter.emit(entity)
    emitter.emit(sanction)


def fsf_parse(context, data):
    emitter = EntityEmitter(context)

    with context.http.rehash(data) as res:
        for entry in res.xml.findall(".//default:sanctionEntity", NS):
            parse_entry(emitter, entry)
    emitter.finalize()
