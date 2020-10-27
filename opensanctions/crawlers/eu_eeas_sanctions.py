from pprint import pprint, pformat  # noqa
import csv
from ftmstore.memorious import EntityEmitter
from followthemoney import model

from opensanctions import constants
from opensanctions.util import jointext

GENDERS = {"M": constants.MALE, "F": constants.FEMALE}


def get_csv_url(context, data):
    with context.http.rehash(data) as result:
        doc = result.html
        csv_link = doc.xpath("//div[@id='dataset-resources']/div/ul/li[3]/span[1]/a")[
            0
        ].get("href")
        data["url"] = csv_link
        context.emit(data=data)


def eeas_parse(context, data):
    emitter = EntityEmitter(context)
    with context.http.rehash(data) as res:
        with open(res.file_path, "r") as csvfile:
            for row in csv.DictReader(csvfile, delimiter=";"):
                parse_entry(context, data, emitter, row)
    emitter.finalize()


def parse_entry(context, data, emitter, entry):
    reg_date = entry.get("Entity_Regulation_EntryIntoForceDate")
    entity = emitter.make("LegalEntity")
    if entry.get("Entity_SubjectType") == "P":
        entity = emitter.make("Person")
    elif entry.get("Entity_SubjectType") == "E":
        entity = emitter.make("Organization")
    entity.id = "eeas-%s" % entry.get("Entity_LogicalId")
    entity.add("sourceUrl", entry.get("Entity_Regulation_PublicationUrl"))
    entity.add("modifiedAt", reg_date)
    entity.context["created_at"] = reg_date

    sanction = emitter.make("Sanction")
    sanction.make_id(entity.id)
    sanction.add("entity", entity)
    sanction.add("authority", "European External Action Service")
    sanction.add("sourceUrl", entry.get("Entity_Regulation_PublicationUrl"))
    sanction.add("program", entry.get("Entity_Regulation_Programme"))
    sanction.add("program", entry.get("Entity_Regulation_NumberTitle"))
    sanction.add("reason", entry.get("Entity_Remark"))
    sanction.add("startDate", reg_date)

    entity.add("name", entry.get("NameAlias_WholeName"))
    if entry.get("Entity_SubjectType") == "P":
        entity.add("title", entry.get("NameAlias_Title"), quiet=True)
        entity.add("firstName", entry.get("NameAlias_FirstName"), quiet=True)
        entity.add("middleName", entry.get("NameAlias_MiddleName"), quiet=True)
        entity.add("lastName", entry.get("NameAlias_LastName"), quiet=True)
        entity.add("position", entry.get("NameAlias_Function"), quiet=True)
        gender = GENDERS.get(entry.get("NameAlias_Gender"))
        entity.add("gender", gender, quiet=True)

    if entry.get("Entity_SubjectType") == "P" and entry.get("Identification_Number"):
        passport = emitter.make("Passport")
        passport.make_id("Passport", entity.id, entry.get("Identification_Number"))
        passport.add("holder", entity)
        passport.add("passportNumber", entry.get("Identification_Number"))
        passport.add("country", entry.get("Identification_CountryIso2Code"))
        emitter.emit(passport)

    address = jointext(
        entry.get("Address_Street"),
        entry.get("Address_PoBox"),
        entry.get("Address_Place"),
        entry.get("Address_City"),
        entry.get("Address_ZipCode"),
    )
    entity.add("address", address)
    entity.add("country", entry.get("Address_CountryIso2Code"))

    if entry.get("Entity_SubjectType") == "P":
        entity.add("birthDate", entry.get("BirthDate_BirthDate"))
        entity.add("birthPlace", entry.get("BirthDate_City"))
        entity.add("birthPlace", entry.get("BirthDate_Place"))
        entity.add("country", entry.get("BirthDate_CountryIso2Code"))

        entity.add("nationality", entry.get("Citizenship_CountryIso2Code"), quiet=True)

    emitter.emit(entity)
    emitter.emit(sanction)

    data["url"] = entry.get("Entity_Regulation_PublicationUrl")
    data["title"] = entry.get("Entity_Regulation_NumberTitle")
    data["entity"] = entity.to_dict()
    context.emit(data=data)


def store(context, data):
    emitter = EntityEmitter(context)
    entity = model.get_proxy(data["entity"])
    documentation = emitter.make("Documentation")
    documentation.make_id("Documentation", data["entity"]["id"], data["aleph_id"])
    documentation.add("entity", entity)
    documentation.add("document", data["aleph_id"])
    emitter.emit(documentation)
    emitter.finalize()
