from pprint import pprint  # noqa
from normality import slugify

from opensanctions import constants
from opensanctions.util import jointext

# https://eeas.europa.eu/topics/sanctions-policy/8442/consolidated-list-of-sanctions_en
# https://webgate.ec.europa.eu/fsd/fsf#!/files

GENDERS = {"M": constants.MALE, "F": constants.FEMALE}

NS = {"default": "http://eu.europa.ec/fpi/fsd/export"}


def parse_entry(context, entry):
    entity = context.make("LegalEntity")
    if entry.find("./default:subjectType", NS).get("classificationCode") == "P":
        entity = context.make("Person")
    reference_no = slugify(entry.get("euReferenceNumber"))
    entity.make_slug(reference_no)

    regulation = entry.find("./default:regulation", NS)
    source_url = regulation.findtext("./default:publicationUrl", "", NS)
    entity.add("sourceUrl", source_url)

    sanction = context.make("Sanction")
    sanction.make_id("Sanction", entity.id)
    sanction.add("entity", entity)
    sanction.add("authority", "European Union")
    sanction.add("sourceUrl", source_url)
    program = jointext(
        regulation.get("programme"),
        regulation.get("numberTitle"),
        sep=" - ",
    )
    sanction.add("program", program)
    sanction.add("reason", entry.findtext("./default:remark", "", NS))
    sanction.add("startDate", regulation.get("entryIntoForceDate"))

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

    # TODO: support other types of ID
    for pnode in entry.findall(
        './default:identification[@identificationTypeCode="passport"]', NS
    ):
        passport = context.make("Passport")
        passport.make_id("Passport", entity.id, pnode.get("logicalId"))
        passport.add("holder", entity)
        passport.add("passportNumber", pnode.get("number"))
        passport.add("country", pnode.get("countryIso2Code"))
        context.emit(passport)

    for node in entry.findall("./default:address", NS):
        address = jointext(
            node.get("street"),
            node.get("city"),
            node.findtext("default:zipCode", "", NS),
        )
        entity.add("address", address)
        entity.add("country", node.get("countryIso2Code"))

    for birth in entry.findall("./default:birthdate", NS):
        entity.add("birthDate", birth.get("birthdate"))
        entity.add("birthPlace", birth.get("city"))

    for country in entry.findall("./default:citizenship", NS):
        entity.add("nationality", country.get("countryIso2Code"), quiet=True)

    context.emit(entity, target=True, unique=True)
    context.emit(sanction)


def crawl(context):
    context.fetch_artifact("source.xml", context.dataset.data.url)
    doc = context.parse_artifact_xml("source.xml")
    for entry in doc.findall(".//default:sanctionEntity", NS):
        parse_entry(context, entry)
