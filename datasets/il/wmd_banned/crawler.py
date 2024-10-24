from zavod import Context
from typing import Dict
from rigour.mime.types import XLSX
import openpyxl
from zavod import helpers as h
import shutil
import re


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


def parse_sheet_row(context: Context, row: Dict):
    record_id = row.pop("record_id_hb")
    if not record_id.isnumeric():  # not a record
        return

    other_info = row.pop("other_info_hb")
    address_nationality = row.pop("address_nationality_hb")

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

    dob = row.pop("dob_hb")
    passport = row.pop("passport_hb")
    parse_name = row.pop("name_hb")

    name_split = h.multi_split(
        parse_name,
        [
            "a)",
            "b)",
            "c)",
            "d)",
            "e)",
            "f)",
            "g)",
            "h)",
            "i)",
            "j)",
            "k)",
            "l)",
            "m)",
            "n)",
            "o)",
            "A.k.a.:",
            "A.K.A.:",
            "ידוע גם:",
            "1:",
            "2:",
            "\n",
        ],
    )
    name = name_split[0]
    alias = name_split[1:] if len(name_split) else None

    isreal_adoption_date = row.pop("isreal_adoption_date_hb")
    serial_no = row.pop("serial_no_hb")
    originally_declared_by = row.pop("originally_declared_by_hb")
    declaration_date = row.pop("declaration_date_hb")

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

    entity.add("name", name)
    entity.add("alias", alias)
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
    context.audit_data(row, ignore=["isreal_temp_adoption_date_hb"])


def crawl_excel_url(context: Context):
    doc = context.fetch_html(
        context.data_url,
    )
    doc.make_links_absolute(
        context.data_url,
    )

    return doc.xpath(
        '//a[contains(@id,"filesToDownload_item")][contains(@href, "xlsx")]'
    )[0].get("href")


def crawl(context: Context):
    try:
        excel_url = crawl_excel_url(context)
        source_path = context.fetch_resource("source.xlsx", excel_url)

    except Exception as e:
        context.log.error(f"Failed to fetch excel file - {e}")

        context.log.warn("Using local copy of the excel file")
        assert context.dataset.base_path is not None
        data_path = context.dataset.base_path / "data.xlsx"
        source_path = context.get_resource_path("source.xlsx")
        shutil.copyfile(data_path, source_path)

    context.export_resource(source_path, XLSX, title=context.SOURCE_TITLE)

    wb = openpyxl.load_workbook(source_path, read_only=True)
    for row in h.parse_xlsx_sheet(
        context,
        wb.active,
        header_lookup="columns",
        skiprows=2,
    ):
        parse_sheet_row(context, row)
