from csv import DictReader
from typing import Dict
from lxml import etree
from zipfile import ZipFile

from rigour.mime.types import CSV

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.shed.fsf import parse_entry

OLD_DATA_URL = (
    "https://financien.belgium.be/sites/default/files/thesaurie/Consolidated%20list.zip"
)


# count of identifiers by number of parts (split on opening paren)
# 1: 4334 - no parentheses
# 2: 609 - single set of parentheses
# 3: 1369
# 4: 387
# ...
# could benefit from LLM extraction e.g.
# "02800443 (id-National identification card) ((identity card))"
# "02810614 (id-National identification card) (ID no)"
# "03 01  118013 (other-Other identification number) (Passport number,  national ID  number, other numbers of  identity  documents: 03 01  118013)",
# "0363464 (passport-National passport)  (issued by Palestinian Authority)"
def apply_identifier(context: Context, entity: Entity, id_number_line: str):
    parts = id_number_line.split("(")
    prop = None

    if len(parts) == 1:
        prop = "idNumber"
    if len(parts) == 2:
        # Intentionally don't set the prop here
        prop = context.lookup_value(
            "identifier_prop",
            parts[1].strip(") "),
            default=prop,
            warn_unmatched=True,
        )

    if prop == "passportNumber":
        entity.add_schema("Person")

    if prop:
        entity.add(prop, parts[0].strip(), original_value=id_number_line)
    else:
        # Replace literal \n mistakes in source with spaces for readability
        entity.add("notes", id_number_line.replace("\\n", " "))


def crawl_row(context: Context, row: Dict[str, str]):
    entity = context.make("LegalEntity")
    name = row.pop("Wholename")
    assert name, row
    birth_date = row.pop("Birth date")
    birth_country = row.pop("Birth country").split("\n")
    entity.id = context.make_id(name, birth_date, birth_country)
    for name in name.split("\n"):
        h.apply_reviewed_names(context, entity, name)
    entity.add_cast("Person", "lastName", row.pop("Lastname").split("\n"))
    entity.add_cast("Person", "firstName", row.pop("Firstname").split("\n"))
    entity.add_cast("Person", "middleName", row.pop("Middlename").split("\n"))
    entity.add_cast("Person", "gender", row.pop("Gender").split("\n"))
    entity.add_cast("Person", "birthPlace", row.pop("Birth place").split("\n"))
    if birth_date:
        entity.add_schema("Person")
        h.apply_date(entity, "birthDate", birth_date)
    entity.add("country", birth_country)
    if position := row.pop("Function").replace("\\n", " "):
        if entity.schema.is_a("Person"):
            entity.add("position", position)
        else:
            entity.add("notes", position)
    # Intentionally don't split literal \n here because in these cases it's not
    # separating id number items, it's embedded within a single id number.
    for id_number_line in row.pop("Number").split("\n"):
        apply_identifier(context, entity, id_number_line)
    entity.add("notes", row.pop("Remark").replace("\\n", " "))
    entity.add("sourceUrl", row.pop("Links").split("\n"))
    entity.add("topics", "sanction")

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "listingDate", row.pop("Publication date"))
    sanction.add(
        "programId",
        h.lookup_sanction_program_key(context, row.pop("Embargos")),
    )
    sanction.add("reason", row.pop("Regulation"))

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row, ["type"])


def crawl_csv(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r", encoding="utf-8-sig") as fh:
        reader = DictReader(fh, delimiter=";")
        for row in reader:
            crawl_row(context, row)


def crawl_old_xml(context: Context):
    path = context.fetch_resource("source.zip", OLD_DATA_URL)
    context.export_resource(path, "application/zip", title=context.SOURCE_TITLE)
    with ZipFile(path, "r") as zip:
        for name in zip.namelist():
            if name.endswith(".xml"):
                with zip.open(name) as fh:
                    doc = etree.parse(fh)
                    doc_ = h.remove_namespace(doc)
                    for entry in doc_.findall(".//sanctionEntity"):
                        parse_entry(context, entry)
                        # print(entry, entry.get("euReferenceNumber"), entry.attrib)


def crawl(context: Context):
    crawl_old_xml(context)
    crawl_csv(context)
