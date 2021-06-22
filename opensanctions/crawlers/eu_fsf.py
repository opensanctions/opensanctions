from pprint import pprint  # noqa
from normality import slugify

from opensanctions.helpers import gender
from opensanctions.util import jointext, remove_namespace

# https://eeas.europa.eu/topics/sanctions-policy/8442/consolidated-list-of-sanctions_en
# https://webgate.ec.europa.eu/fsd/fsf#!/files

GENDERS = {"M": gender.MALE, "F": gender.FEMALE}


def parse_entry(context, entry):
    entity = context.make("LegalEntity")
    if entry.find("./subjectType").get("classificationCode") == "P":
        entity = context.make("Person")
    reference_no = slugify(entry.get("euReferenceNumber"))
    entity.make_slug(reference_no)

    regulation = entry.find("./regulation")
    source_url = regulation.findtext("./publicationUrl", "")
    entity.add("sourceUrl", source_url)

    sanction = context.make_sanction(entity)
    program = jointext(
        regulation.get("programme"),
        regulation.get("numberTitle"),
        sep=" - ",
    )
    sanction.add("program", program)
    sanction.add("reason", entry.findtext("./remark", ""))
    sanction.add("startDate", regulation.get("entryIntoForceDate"))

    for name in entry.findall("./nameAlias"):
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
    for pnode in entry.findall('./identification[@identificationTypeCode="passport"]'):
        passport = context.make("Passport")
        passport.make_id("Passport", entity.id, pnode.get("logicalId"))
        passport.add("holder", entity)
        passport.add("passportNumber", pnode.get("number"))
        passport.add("country", pnode.get("countryIso2Code"))
        context.emit(passport)

    for node in entry.findall("./address"):
        address = jointext(
            node.get("street"),
            node.get("city"),
            node.findtext("zipCode", ""),
        )
        entity.add("address", address)
        entity.add("country", node.get("countryIso2Code"))

    for birth in entry.findall("./birthdate"):
        entity.add("birthDate", birth.get("birthdate"))
        entity.add("birthPlace", birth.get("city"))

    for country in entry.findall("./citizenship"):
        entity.add("nationality", country.get("countryIso2Code"), quiet=True)

    context.emit(entity, target=True, unique=True)
    context.emit(sanction)


def crawl(context):
    context.fetch_resource("source.xml", context.dataset.data.url)
    doc = context.parse_resource_xml("source.xml")
    doc = remove_namespace(doc)
    for entry in doc.findall(".//sanctionEntity"):
        parse_entry(context, entry)
