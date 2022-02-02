import re
from datetime import datetime
from followthemoney.proxy import P
from openpyxl import load_workbook
from pantomime.types import XLSX
from normality import slugify, stringify

from opensanctions.core import Context
from opensanctions import helpers as h
from opensanctions.util import multi_split

ORG_URL = "https://nbctf.mod.gov.il/he/Announcements/Documents/NBCTFIsrael%20-%20Terror%20Organization%20Designation%20List_XL.xlsx"
PEOPLE_URL = "https://nbctf.mod.gov.il/he/Announcements/Documents/NBCTF%20Israel%20designation%20Individuals_XL.xlsx"
FORMATS = ["%d/%m/%Y", "%d.%m.%Y", "%Y-%m-%d"]
NA_VALUE = re.compile(r"^[\-\/]+$")
END_TAG = re.compile(r"בוטל ביום", re.U)


def parse_date(date):
    dates = []
    for part in multi_split(date, ["OR", ";", " - "]):
        dates.extend(h.parse_date(part, FORMATS))
    return dates


def parse_interval(entity, date):
    if date is None:
        return
    date = date.strip()
    if "בוטל ביום" in date:
        date, _ = date.rsplit(" ", 1)
        entity.add("endDate", parse_date(date))
    else:
        entity.add("startDate", parse_date(date))


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
        entity = context.make("Person")
        entity.id = context.make_id(name_en, name_he, name_ar)
        if entity.id is None:
            continue
        entity.add("name", name_en or name_he or name_ar)
        entity.add("alias", name_he)
        entity.add("alias", name_ar)
        entity.add("topics", "crime.terror")
        entity.add("birthDate", parse_date(record.pop("d_o_b", None)))
        entity.add("nationality", record.pop("nationality_residency", None))
        entity.add("idNumber", record.pop("individual_id", None))

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

        context.emit(entity, target=True)
        context.emit(sanction)
        h.audit_data(record)


def crawl_organizations(context: Context):
    path = context.fetch_resource("organizations.xlsx", ORG_URL)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)
    seq_ids = {}
    links = []
    for record in excel_records(path):
        seq_id = record.pop("internal_seq_id", None)
        name_en = record.pop("organization_name_english", None)
        name_he = record.pop("organization_name_hebrew", None)
        entity = context.make("Organization")
        entity.id = context.make_id(name_en, name_he)
        if entity.id is None:
            continue
        if seq_id is not None:
            seq_ids[seq_id] = entity.id
        entity.add("name", name_en)
        entity.add("name", name_he)
        entity.add("topics", "crime.terror")
        entity.add("notes", lang_pick(record, "comments"))
        entity.add("notes", record.pop("column_42", None))
        entity.add("email", record.pop("email", None))
        entity.add("country", record.pop("country_hebrew", None))
        entity.add("country", record.pop("country_english", None))
        entity.add("registrationNumber", record.pop("corporation_id", None))
        entity.add("legalForm", lang_pick(record, "corporation_type"))
        entity.add("jurisdiction", lang_pick(record, "location_of_formation"))
        date = parse_date(record.pop("date_of_corporation", None))
        entity.add("incorporationDate", date)
        for field in list(record.keys()):
            if field.startswith("organization_name_"):
                entity.add("alias", record.pop(field, None))
            if field.startswith("telephone"):
                entity.add("phone", record.pop(field, None))
            if field.startswith("website"):
                entity.add("website", record.pop(field, None))

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
        if street or city:
            address = h.make_address(
                context, street=street, city=city, country_code=entity.first("country")
            )
            h.apply_address(context, entity, address)

        for field in (
            "date_of_temporary_designation",
            "date_of_permenant_designation",
            "date_designation_in_west_bank",
        ):
            parse_interval(sanction, record.pop(field, None))

        context.emit(entity, target=True)
        context.emit(sanction)
        h.audit_data(record)

    for (subject, object) in links:
        subject_id = seq_ids.get(subject)
        object_id = seq_ids.get(object)
        if subject_id is None or object_id is None:
            continue
        link = context.make("UnknownLink")
        link.id = context.make_id(subject_id, object_id)
        link.add("subject", subject_id)
        link.add("object", object_id)
        context.emit(link)
