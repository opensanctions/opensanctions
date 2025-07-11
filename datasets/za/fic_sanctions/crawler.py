from typing import Dict, List
import re

from zavod import Context
from zavod import helpers as h


REGEX_PASSPORT = re.compile(r"^[A-Z0-9-]{6,20}$")
ALIAS_SPLITS = [
    "a.k.a.,",
    "f.k.a.,",
    "; ",
    "f.k.a,",
    "f.n.a.,",
    "Formerly known as,",
    " Good,",
    "Formerly Known As,",
]
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
    "v)",
    "iv)",
    "iii)",
    "ii)",
    "i)",
    "(Formerly located at)",
]


def clean_passports(context: Context, text: str) -> List[str]:
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


def crawl_row(context: Context, data: Dict[str, str]):
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
    entity.add("name", full_name)
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

        aliases = data.pop("IndividualAlias", None)
        for alias in h.multi_split(aliases, [", ", "Good", "Low"]):
            if all(c in {"?", " "} for c in alias):
                continue
            entity.add("alias", alias)
            if "?" in entity.get("alias"):
                context.log.warning("Alias contains '?'", alias=alias)
        passports, ids = clean_passports(context, data.pop("IndividualDocument", ""))
        entity.add("passportNumber", passports)
        entity.add("idNumber", ids)

    if entity.schema.is_a("LegalEntity"):
        address = data.pop("EntityAddress", None)
        for address in h.multi_split(address, ADDRESS_SPLITS):
            address = address.rstrip(",")
            entity.add("address", address)
        aliases = data.pop("EntityAlias", None)
        for alias in h.multi_split(aliases, ALIAS_SPLITS):
            # if we split on a comma, we will separate ", LTD" from the name
            alias = alias.rstrip(",")
            if all(c in {"?", " "} for c in alias):
                continue
            entity.add("alias", alias)
            if "?" in entity.get("alias"):
                context.log.warning("Alias contains '?'", alias=alias)
    listed_on = data.pop("ListedOn", None)
    h.apply_date(entity, "createdAt", listed_on)

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "listingDate", listed_on)
    sanction.add("unscId", data.pop("ReferenceNumber", None))

    context.audit_data(data, ignore=["ApplicationStatus"])
    context.emit(entity)
    context.emit(sanction)


def crawl(context: Context):
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
            for field in row.getchildren():
                value = field.text
                if value == "NA":
                    continue
                data[field.tag] = value
            crawl_row(context, data)
    potential_table = doc.find(".//Table2")
    if potential_table is not None:
        context.log.warning("Table2 found in source, but not yet implemented")
