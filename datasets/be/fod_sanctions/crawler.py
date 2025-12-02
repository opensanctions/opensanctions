from csv import DictReader
from typing import Dict, List
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
        prop = context.lookup_value(
            "identifier_prop", parts[1].strip(") "), warn_unmatched=True
        )

    if prop == "passportNumber":
        entity.add_schema("Person")

    if prop:
        entity.add(prop, parts[0].strip(), original_value=id_number_line)
    else:
        entity.add("notes", id_number_line)


def crawl_row(context: Context, entity_id: str | None, row: Dict[str, List[str]]):
    entity = context.make("LegalEntity")
    entity.id = entity_id

    whole_names = row.pop("Wholename")
    assert whole_names, row
    for name in whole_names:
        h.apply_reviewed_names(context, entity, name)
    entity.add_cast("Person", "lastName", row.pop("Lastname"))
    entity.add_cast("Person", "firstName", row.pop("Firstname"))
    entity.add_cast("Person", "middleName", row.pop("Middlename"))
    entity.add_cast("Person", "gender", row.pop("Gender"))
    entity.add_cast("Person", "birthPlace", row.pop("Birth place"))
    for birth_date in row.pop("Birth date"):
        entity.add_schema("Person")
        h.apply_date(entity, "birthDate", birth_date)
    entity.add("country", row.pop("Birth country"))
    if position := row.pop("Function"):
        if entity.schema.is_a("Person"):
            entity.add("position", position)
        else:
            entity.add("notes", position)
    for id_number_line in row.pop("Number"):
        apply_identifier(context, entity, id_number_line)
    entity.add("notes", row.pop("Remark"))
    entity.add("sourceUrl", row.pop("Links"))
    entity.add("topics", "sanction")

    sanction = h.make_sanction(context, entity)
    for listing_date in row.pop("Publication date"):
        h.apply_date(sanction, "listingDate", listing_date)
    for embargo in row.pop("Embargos"):
        program_id = h.lookup_sanction_program_key(context, embargo)
        sanction.add("programId", program_id)
    sanction.add("reason", row.pop("Regulation"))

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row, ["type"])


def crawl_csv(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    record_count = 0
    with open(path, "r", encoding="utf-8-sig") as fh:
        reader = DictReader(fh, delimiter=";")
        for row in reader:
            # Use raw values before any splitting/replacing for id
            name = row.get("Wholename")
            birth_date = row.get("Birth date")
            birth_country = row.get("Birth country")
            entity_id = context.make_id(name, birth_date, birth_country)
            # They split distinct values with newlines. They also have some
            # literal backslash-n sequences which are some kind of data handling mistake
            # and not delimiting distinct values.
            split_row = {k: v.replace("\\n", " ").split("\n") for k, v in row.items()}
            crawl_row(context, entity_id, split_row)
            record_count += 1
    context.log.info(f"Crawled {record_count} CSV records.")


def crawl_old_xml(context: Context):
    path = context.fetch_resource("source.zip", OLD_DATA_URL)
    context.export_resource(path, "application/zip", title=context.SOURCE_TITLE)
    record_count = 0
    with ZipFile(path, "r") as zip:
        for name in zip.namelist():
            if name.endswith(".xml"):
                with zip.open(name) as fh:
                    doc = etree.parse(fh)
                    doc_ = h.remove_namespace(doc)
                    for entry in doc_.findall(".//sanctionEntity"):
                        parse_entry(context, entry)
                        record_count += 1
    context.log.info(f"Crawled {record_count} XML records.")


def crawl(context: Context):
    crawl_old_xml(context)
    crawl_csv(context)
