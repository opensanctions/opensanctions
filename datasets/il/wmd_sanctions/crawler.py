from rigour.mime.types import XLSX
from typing import Dict
from zavod import helpers as h
import openpyxl
import re

from zavod import Context
from zavod.entity import Entity
from zavod.shed.zyte_api import fetch_html, fetch_resource

# "a.k.a.", "A.K.A.:" and variations
# ידוע גם ("also known as")
# ועוד ידוע: ("and also known")
# a), b), ...
REGEX_NAME_SPLIT = r"\b[AF]\.?K\.?A[:\.\b]+|;|ידוע גם:?|ועוד ידוע:?|\b[a-z]\)"
SKIP_ROWS = {
    'הכרזות – איראן יחידים – סה"כ 23 גורמים מוכרזים:',
    'הכרזות – איראן ארגונים/קבוצות – סה"כ 61 גורמים מוכרזים:',
    'הכרזות – צפון קוריאה יחידים – סה"כ 80 גורמים מוכרזים:',
    'הכרזות – צפון קוריאה ארגונים/קבוצות – סה"כ 75 גורמים מוכרזים:',
}
SPLITS = ["a)", "b)", "c)", "d)", "e)"]
CHOPSKA = [
    ("מספר זיהוי:", "idNumber"),
    ("מספר דרכון:", "passportNumber"),
    ("Passport no:", "passportNumber"),
]


def apply_identifiers(entity: Entity, text: str):
    """
    Split and add passport and ID numbers but don't try and parse fully.
    """
    if not text:
        return None
    if "n/a" in text.lower():
        return None
    items = h.multi_split(text, SPLITS)
    for item in items:
        for chop, prop in CHOPSKA:
            parts = item.rsplit(chop, 1)
            item = parts[0]
            if len(parts) > 1:
                entity.add(prop, parts[1].strip())
        entity.add("passportNumber", item.strip())


def extract_n_pop_address(text: str):
    """
    Extract address and update the text by removing the extracted address.
    """
    if not text:
        return None, None

    pattern = r"(address|location):\s*(.*)"
    match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
    if match:
        address = match.group(2).strip()

        # Remove the extracted location from the original text
        updated_text = re.sub(
            pattern, "", text, flags=re.DOTALL | re.IGNORECASE
        ).strip()

        return address, updated_text

    else:
        return None, text


def clean_date(date_str: str):
    """
    Clean the  date string by replacing newlines, colons, and dots with a
    space character so it matches date pattern and return a multi split of dates
    """
    if not date_str:
        return []
    date_str = str(date_str).lower()
    date_str = re.sub(r"[:\n\.]|dob|\xa0", " ", date_str).strip()

    return h.multi_split(date_str, ["a)", " b)", "c)", "d)", "e)", "and", "between"])


def apply_names(context: Context, entity: Entity, names_string: str) -> None:
    name_split = re.split(REGEX_NAME_SPLIT, names_string, flags=re.IGNORECASE)
    name = name_split[0]
    name = re.sub(r"\d:\s*(na)?", "", name)
    h.apply_name(entity, full=name)

    for alias in name_split[1:]:
        h.apply_name(entity, full=alias, alias=True)


def crawl_row(context: Context, row: Dict):
    record_id = row.pop("record_id")
    if not record_id.isnumeric():  # not a record
        if record_id not in SKIP_ROWS:
            context.log.warning(
                "Skipping unexpected row that doesn't look like a record",
                record_id=record_id,
            )
        return

    names_string = row.pop("name")
    serial_no = row.pop("serial_no")

    schema = context.lookup_value("schema", serial_no)
    if schema is None:
        context.log.warning(
            "Entity type not recognized from serial number", serial_no=serial_no
        )
        schema = "LegalEntity"

    entity = context.make(schema)
    entity.id = context.make_id(serial_no, names_string)
    apply_names(context, entity, names_string)
    if entity.schema.is_a("Person"):
        h.apply_dates(entity, "birthDate", clean_date(row.pop("birth_incorp_date")))
        apply_identifiers(entity, row.pop("passports_ids"))
    elif entity.schema.is_a("Organization"):
        h.apply_dates(
            entity, "incorporationDate", clean_date(row.pop("birth_incorp_date"))
        )

    # Extract address if it exists from either the info or nationality attibrutes
    info_address, notes = extract_n_pop_address(row.pop("other_info"))
    nat_address, country = extract_n_pop_address(row.pop("address_nationality"))

    entity.add("country", country)
    entity.add("notes", notes)
    entity.add("address", h.multi_split(info_address, SPLITS))
    entity.add("address", h.multi_split(nat_address, SPLITS))
    entity.add("topics", "sanction")

    sanction = h.make_sanction(context, entity)
    h.apply_dates(
        sanction, "startDate", clean_date(row.pop("isreal_temp_adoption_date"))
    )
    h.apply_dates(sanction, "startDate", clean_date(row.pop("isreal_adoption_date")))

    context.emit(entity)
    context.emit(sanction)
    context.audit_data(row, ignore=["declaration_date", "originally_declared_by"])


def crawl_excel_url(context: Context):
    file_xpath = '//a[contains(@id,"filesToDownload_item")][contains(@href, "xlsx")]'
    doc = fetch_html(
        context, context.data_url, file_xpath, cache_days=1, absolute_links=True
    )

    return doc.xpath(file_xpath)[0].get("href")


def crawl(context: Context):
    excel_url = crawl_excel_url(context)
    _, _, _, source_path = fetch_resource(
        context, "source.xlsx", excel_url, expected_media_type=XLSX
    )
    context.export_resource(source_path, XLSX, title=context.SOURCE_TITLE)

    wb = openpyxl.load_workbook(source_path, read_only=True)
    for row_num, row in enumerate(
        h.parse_xlsx_sheet(
            context, wb.active, header_lookup=context.get_lookup("columns"), skiprows=2
        )
    ):
        # Check that additional header rows are consistent with first row
        if row["record_id"] == 'מס"ד':
            for key, value in row.items():
                translated_header = context.lookup_value("columns", value)
                assert translated_header == key, (
                    row_num,
                    key,
                    translated_header,
                    value,
                )
            continue

        crawl_row(context, row)
