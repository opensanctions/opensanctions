from rigour.mime.types import XLSX
from typing import Dict
from zavod import helpers as h
import openpyxl
import re

from zavod import Context
from zavod.entity import Entity
from zavod.shed.zyte_api import fetch_html, fetch_resource

# a.k.a. and variations
# ידוע גם ("also known as")
# ועוד ידוע: ("and also known")
# a), b), ...
REGEX_NAME_SPLIT = r"\b[AF]\.?K\.?A[:\.\b]|;|ידוע גם:?|ועוד ידוע:?|\b[a-z]\)"


def extract_passport_no(text: str):
    """
    Extract passport numbers from a given text.
    """
    if not text:
        return None
    text = str(text)
    pattern = r"\b[A-Z0-9]{5,}\b"
    matches = re.findall(pattern, text)

    return matches


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
    aliases = name_split[1:]
    parts = name.split("2:")
    if entity.schema.is_a("Person") and len(parts) > 1:
        last_name = parts[0].replace("1:", "").strip()
        forenames_str = re.sub(r"\d:\s*na", "", parts[1])
        forenames = re.split(r"\d:", forenames_str)
        first_name = forenames.pop(0).strip()
        second_name = forenames.pop(0).strip() if forenames else None
        if forenames:
            context.log.warn("More names found", names=forenames)
        h.apply_name(
            entity, first_name=first_name, second_name=second_name, last_name=last_name
        )
    else:
        h.apply_name(entity, full=name)
    for alias in aliases:
        h.apply_name(entity, full=alias, alias=True)


def parse_sheet_row(context: Context, row: Dict):
    record_id = row.pop("record_id")
    if not record_id.isnumeric():  # not a record
        return

    other_info = row.pop("other_info")
    address_nationality = row.pop("address_nationality")

    # Extract address if it exists from either the info or nationality attibrutes
    info_address, notes = extract_n_pop_address(other_info)
    nat_address, nationality = extract_n_pop_address(address_nationality)

    address = info_address or nat_address
    address = h.multi_split(
        address,
        [
            "a)",
            "b)",
            "c)",
            "d)",
            "e)",
        ],
    )

    dob = row.pop("dob")
    passport = row.pop("passport")
    names_string = row.pop("name")

    isreal_adoption_date = row.pop("isreal_adoption_date")
    serial_no = row.pop("serial_no")
    originally_declared_by = row.pop("originally_declared_by")
    declaration_date = row.pop("declaration_date")

    if "iri" in serial_no.lower() or "pi" in serial_no.lower():
        entity = context.make("Person")
        entity.id = context.make_id("Person", f"{record_id}-{serial_no}")
        entity.add("passportNumber", extract_passport_no(passport))
        entity.add("nationality", nationality)
        h.apply_dates(
            entity,
            "birthDate",
            clean_date(dob),
        )

    elif "ire" in serial_no.lower() or "pe" in serial_no.lower():
        entity = context.make("Organization")
        entity.id = context.make_id("Company", f"{record_id}-{serial_no}")

    else:
        context.log.warn(f"Entity not recognized from serial number: {serial_no}")

    apply_names(context, entity, names_string)
    entity.add("notes", notes)
    entity.add("address", address)
    entity.add("topics", "sanction")

    sanction = h.make_sanction(context, entity)
    sanction.add("authority", originally_declared_by, lang="he")
    h.apply_dates(sanction, "listingDate", clean_date(declaration_date))
    h.apply_dates(
        sanction,
        "startDate",
        clean_date(isreal_adoption_date),
    )

    context.emit(entity, target=True)
    context.emit(sanction)
    context.audit_data(row, ignore=["isreal_temp_adoption_date"])


def unblock_validator(doc):
    return len(doc.xpath("//article")) > 0


def crawl_excel_url(context: Context):
    doc = fetch_html(context, context.data_url, unblock_validator, cache_days=1)
    doc.make_links_absolute(context.data_url)

    return doc.xpath(
        '//a[contains(@id,"filesToDownload_item")][contains(@href, "xlsx")]'
    )[0].get("href")


def crawl(context: Context):
    excel_url = crawl_excel_url(context)
    cached, source_path, media_type, _ = fetch_resource(
        context, "source.xlsx", excel_url
    )
    if not cached:
        assert media_type == XLSX, media_type
    context.export_resource(source_path, XLSX, title=context.SOURCE_TITLE)

    wb = openpyxl.load_workbook(source_path, read_only=True)
    for row in h.parse_xlsx_sheet(
        context,
        wb.active,
        header_lookup="columns",
        skiprows=2,
    ):
        parse_sheet_row(context, row)
