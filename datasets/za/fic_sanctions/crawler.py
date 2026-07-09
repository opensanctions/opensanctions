import re

from zavod import Context
from zavod import helpers as h


REGEX_PASSPORT = re.compile(r"^[A-Z0-9-]{6,20}$")
ADDRESS_SPLITS = [
    "Branch Office 1:",
    "Branch Office 2:",
    "Branch Office 3:",
    "Branch Office 4:",
    "Branch Office 5:",
    "Branch Office 6:",
    "Branch Office 7:",
    "Branch Office 8:",
    "Branch Office 9:",
    "Branch Office 10:",
    "Branch Office 11:",
    "Branch Office 12:",
    "Branch Office 13:",
    "Branch Office 14:",
    "Branch Office 15:",
    "Branch Office 16:",
    "iii)",
    "iv)",
    "ii)",
    "v)",
    "i)",
    "(Formerly located at)",
]


def clean_passports(context: Context, text: str) -> tuple[list[str], list[str]]:
    # Returns (passport_numbers, national_id_numbers)
    values = text.split(", ")
    passports = []
    ids = []
    is_id = None
    for value in values:
        if not value:
            continue
        if value.lower() == "national identification number":
            is_id = True
        elif value.lower() in "passport":
            is_id = False
        elif REGEX_PASSPORT.search(value):
            if is_id:
                ids.append(value)
            else:
                passports.append(value)
            is_id = None
        else:
            passports.append(value)
            is_id = None
    return passports, ids


def crawl_row(context: Context, data: dict[str, str]) -> None:
    full_name = data.pop("FullName", None)
    if full_name is not None:
        ent_id = data.pop("IndividualID")
        schema = "Person"
    else:
        full_name = data.pop("FirstName")
        ent_id = data.pop("EntityID")
        schema = "LegalEntity"
    entity = context.make(schema)
    entity.id = context.make_slug(ent_id, full_name)
    assert entity.id, data
    names = h.Names(name=full_name)
    entity.add("topics", "sanction")
    entity.add("notes", h.clean_note(data.pop("Comments", None)))
    if entity.schema.is_a("Person"):
        entity.add("address", data.pop("IndividualAddress", None))
        entity.add("nationality", data.pop("Nationality", None))
        entity.add("title", data.pop("Title", None))
        entity.add("position", data.pop("Designation", None))
        entity.add("birthPlace", data.pop("IndividualPlaceOfBirth", None))
        dob = data.pop("IndividualDateOfBirth", None)
        h.apply_date(entity, "birthDate", dob)
        names.add("alias", data.pop("IndividualAlias", None))
        passports, ids = clean_passports(context, data.pop("IndividualDocument", ""))
        entity.add("passportNumber", passports)
        entity.add("idNumber", ids)

    if entity.schema.is_a("LegalEntity"):
        address = data.pop("EntityAddress", None)
        for address in h.multi_split(address, ADDRESS_SPLITS):
            address = address.rstrip(",")
            entity.add("address", address)
        names.add("alias", data.pop("EntityAlias", None))

    h.apply_reviewed_names(context, entity, original=names)
    listed_on = data.pop("ListedOn", None)
    h.apply_date(entity, "createdAt", listed_on)

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "listingDate", listed_on)
    sanction.add("unscId", data.pop("ReferenceNumber", None))

    context.audit_data(data, ignore=["ApplicationStatus"])
    context.emit(entity)
    context.emit(sanction)


def crawl(context: Context) -> None:
    path = context.fetch_resource(
        "source.xml",
        context.data_url,
        method="POST",
        data={"fileType": "xml"},
    )
    context.export_resource(path, "text/xml", title=context.SOURCE_TITLE)
    doc = context.parse_resource_xml(path)
    tables = [".//Table", ".//Table1"]
    for table in tables:
        for row in doc.findall(table):
            data = {}
            for field in row.iterchildren():
                value = field.text
                if value is None or value == "NA":
                    continue
                data[field.tag] = value
            crawl_row(context, data)
    potential_table = doc.find(".//Table2")
    if potential_table is not None:
        context.log.warning("Table2 found in source, but not yet implemented")
