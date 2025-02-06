import re
from datetime import datetime
from openpyxl import load_workbook
from rigour.mime.types import XLSX
from normality import slugify, stringify

from zavod import Context
from zavod import helpers as h

ORG_URL = "https://nbctf.mod.gov.il/he/Announcements/Documents/NBCTFIsrael%20-%20Terror%20Organization%20Designation%20List_XL.xlsx"
PEOPLE_URL = "https://nbctf.mod.gov.il/he/Announcements/Documents/NBCTF%20Israel%20designation%20Individuals_XL.xlsx"
NA_VALUE = re.compile(r"^[\-\/]+$")
END_TAG = re.compile(r"בוטל ביום", re.U)
SPLITS = ["; ", "Id Number", "a) ", "b) ", "c) ", " :", "\n"]
DATE_SPLITS = ["OR", ";", " - ", "a) ", "b) ", "c) "]


def parse_interval(sanction, date):
    if date is None:
        return
    date = date.strip()
    if "בוטל ביום" in date:
        date, _ = date.rsplit(" ", 1)
        h.apply_date(sanction, "endDate", _)
    else:
        for part in h.multi_split(date, DATE_SPLITS):
            h.apply_date(sanction, "startDate", part)


def lang_pick(record, field):
    hebrew = record.pop(f"{field}_hebrew", None)
    english = record.pop(f"{field}_english", None)
    if english is not None:
        return english
    return hebrew


def header_names(cells):
    headers = []
    for idx, cell in enumerate(cells):
        if cell is None:
            cell = f"column_{idx}"
        cell = cell.replace("(DD/MM/YYYY)", "")
        headers.append(slugify(cell, "_"))
    return headers


def excel_records(path):
    wb = load_workbook(filename=path, read_only=True)
    for sheet in wb.worksheets:
        headers = None
        for idx, row in enumerate(sheet.rows):
            cells = [c.value for c in row]
            if headers is not None:
                record = {}
                for header, value in zip(headers, cells):
                    if isinstance(value, datetime):
                        value = value.date()
                    value = stringify(value)
                    if value is not None and NA_VALUE.match(value) is None:
                        record[header] = value
                yield record

            if idx == 1:
                headers = header_names(cells)


def crawl(context: Context):
    crawl_organizations(context)
    crawl_individuals(context)


def crawl_individuals(context: Context):
    path = context.fetch_resource("individuals.xlsx", PEOPLE_URL)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)
    for record in excel_records(path):
        seq_id = record.pop("internal_seq_id", None)
        if seq_id is None:
            continue
        name_en = record.pop("name_of_individual_english", None)
        name_he = record.pop("name_of_individual_hebrew", None)
        name_ar = record.pop("name_of_individual_arabic", None)
        name_en = name_en.replace('="---"', "") if name_en else None
        name_he = name_he.replace('="---"', "") if name_he else None
        name_ar = name_ar.replace('="---"', "") if name_ar else None
        entity = context.make("Person")
        entity.id = context.make_id(name_en, name_he, name_ar)
        if entity.id is None:
            continue
        entity.add("name", name_en, lang="eng")
        entity.add("name", name_he, lang="heb")
        entity.add("name", name_ar, lang="ara")
        entity.add("topics", "crime.terror")
        for part in h.multi_split(record.pop("d_o_b", None), DATE_SPLITS):
            h.apply_date(entity, "birthDate", part)
        entity.add("nationality", record.pop("nationality_residency", None))
        id_number = record.pop("individual_id", "")
        id_number = id_number.replace(":\n", ": ")
        entity.add("idNumber", h.multi_split(id_number, SPLITS))

        sanction = h.make_sanction(context, entity)
        sanction.add("recordId", seq_id)
        sanction.add("recordId", record.pop("foreign_designation_id", None))
        sanction.add("program", record.pop("designation", None))
        sanction.add("program", record.pop("foreign_designation", None))
        sanction.add("authority", lang_pick(record, "designated_by"))

        lang_pick(record, "designated_by_abroad")
        record.pop("date_of_foreign_designation_date", None)

        for field in ("date_of_designation_in_israel",):
            parse_interval(sanction, record.pop(field, None))

        context.emit(entity)
        context.emit(sanction)
        context.audit_data(record)


def crawl_organizations(context: Context):
    path = context.fetch_resource("organizations.xlsx", ORG_URL)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)
    seq_ids = {}
    links = []
    for record in excel_records(path):
        seq_id = record.pop("internal_seq_id", None)
        name_en = record.pop("organization_name_english", None)
        name_he = record.pop("organization_name_hebrew", None)
        name_en = name_en.replace('="---"', "") if name_en else None
        name_he = name_he.replace('="---"', "") if name_he else None
        entity = context.make("Organization")
        entity.id = context.make_id(name_en, name_he)
        if entity.id is None:
            continue
        if seq_id is not None:
            seq_ids[seq_id] = entity.id
        entity.add("name", name_en, lang="eng")
        entity.add("name", name_he, lang="heb")
        entity.add("topics", "crime.terror")
        entity.add("notes", h.clean_note(lang_pick(record, "comments")))
        entity.add("notes", h.clean_note(record.pop("column_42", None)))
        entity.add("notes", h.clean_note(record.pop("column_39", None)))
        entity.add("email", record.pop("email", None))
        entity.add("country", record.pop("country_hebrew", None), lang="heb")
        entity.add("country", record.pop("country_english", None), lang="eng")
        entity.add("registrationNumber", record.pop("corporation_id", None))
        entity.add("legalForm", lang_pick(record, "corporation_type"))
        entity.add("jurisdiction", lang_pick(record, "location_of_formation"))
        for part in h.multi_split(record.pop("date_of_corporation", None), DATE_SPLITS):
            h.apply_date(entity, "incorporationDate", part)
        for field in list(record.keys()):
            if field.startswith("organization_name_"):
                entity.add("alias", h.multi_split(record.pop(field, None), SPLITS))
            if field.startswith("telephone"):
                entity.add("phone", record.pop(field, None))
            if field.startswith("website"):
                entity.add("website", record.pop(field, None))

        entity.add("phone", record.pop("column_67", None))
        entity.add("phone", record.pop("column_70", None))
        entity.add("website", record.pop("column_73", None))

        sanction = h.make_sanction(context, entity)
        sanction.add("recordId", seq_id)
        sanction.add("recordId", record.pop("seq_num_in_other_countries", None))
        sanction.add("program", record.pop("designation_type", None))
        sanction.add("reason", lang_pick(record, "designation_justification"))
        sanction.add("authority", lang_pick(record, "designated_by"))
        sanction.add("publisher", record.pop("public_records_references", None))

        lang_pick(record, "designated_by_abroad")
        record.pop("date_designated_in_other_countries", None)

        linked = record.pop("linked_to_internal_seq_id", "")
        for link in linked.split(";"):
            links.append((max(link, seq_id), min(link, seq_id)))

        street = lang_pick(record, "street")
        city = lang_pick(record, "city_village")
        street = street.replace('="---"', "") if street else None
        city = city.replace('="---"', "") if city else None
        if street or city:
            address = h.make_address(
                context,
                street=street,
                city=city,
                country_code=entity.first("country"),
            )
            h.apply_address(context, entity, address)

        for field in (
            "date_of_temporary_designation",
            "date_of_permenant_designation",
            "date_designation_in_west_bank",
        ):
            parse_interval(sanction, record.pop(field, None))

        operatives = record.pop("key_operatives", None)
        if operatives:
            res = context.lookup("key_operatives", operatives)
            if res:
                for item in res.operatives:
                    operative = context.make(item.pop("schema", "LegalEntity"))
                    operative.id = context.make_id(
                        entity.id, item["name"], item.get("country", None)
                    )
                    for key, value in item.items():
                        operative.add(key, value)
                    rel = context.make("UnknownLink")
                    rel.id = context.make_id(entity.id, operative.id)
                    rel.add("subject", entity.id)
                    rel.add("object", operative.id)
                    rel.add("role", "Key operative")
                    context.emit(operative)
                    context.emit(rel)
            else:
                context.log.warning("Unhandled key_operatives", value=operatives)

        context.emit(entity)
        context.emit(sanction)
        context.audit_data(record)

    for subject, object in links:
        subject_id = seq_ids.get(subject)
        object_id = seq_ids.get(object)
        if subject_id is None or object_id is None:
            continue
        link = context.make("UnknownLink")
        link.id = context.make_id(subject_id, object_id)
        link.add("subject", subject_id)
        link.add("object", object_id)
        context.emit(link)
