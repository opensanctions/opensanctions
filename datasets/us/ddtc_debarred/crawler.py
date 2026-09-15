import openpyxl
from rigour.mime.types import XLSX
from typing import Any

from zavod import Context
from zavod import helpers as h

STATUTORY_XLSX_URL = "https://www.pmddtc.state.gov/sys_attachment.do?sys_id=27c46b251baf29102b6ca932f54bcb20"
ADMINISTRATIVE_XLSX_URL = "https://www.pmddtc.state.gov/sys_attachment.do?sys_id=9f8bbc2f1b8f29d0c6c3866ae54bcbdb"


def crawl_debarment(
    context: Context,
    row: dict[str, Any],
    program_name: str,
    name_field: str,
    notice_date_field: str,
) -> None:
    date_of_birth = row.pop("date_of_birth", None)
    if date_of_birth:
        schema = "Person"
    else:
        schema = "LegalEntity"

    entity = context.make(schema)
    name = row.pop(name_field)
    entity.id = context.make_id(name, date_of_birth)

    entity.add("country", "us")
    if schema == "Person":
        h.apply_date(entity, "birthDate", date_of_birth)
    entity.add("topics", "debarment")

    original = h.Names(name=name)
    h.apply_reviewed_names(
        context,
        entity,
        original=original,
        llm_cleaning=True,
    )

    # Statutory and administrative debarment are two entry paths into the same
    # ITAR § 127.7 regime, so both lists map to the US-AECA-DEBARRED program.
    sanction = h.make_sanction(
        context,
        entity,
        program_name=program_name,
        program_key="US-AECA-DEBARRED",
    )
    sanction.add("listingDate", row.pop(notice_date_field))
    sanction.add("listingDate", row.pop("corrected_notice_date", None))
    sanction.add(
        "description",
        "Federal register notice: " + row.pop("federal_register_notice"),
    )
    corrected_notice_number = row.pop("corrected_notice", None)
    if corrected_notice_number:
        sanction.add(
            "description",
            "Corrected federal register notice: " + corrected_notice_number,
        )

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row, ["charging_letter", "debarment_order"])


def crawl(context: Context) -> None:
    path = context.fetch_resource("statutory.xlsx", STATUTORY_XLSX_URL)
    context.export_resource(path, XLSX, title="Statutory Debarments")
    wb = openpyxl.load_workbook(path, read_only=True)
    for row in h.parse_xlsx_sheet(context, wb.worksheets[0]):
        crawl_debarment(
            context, row, "Statutorily Debarred Parties", "party_name", "notice_date"
        )

    path = context.fetch_resource("administrative.xlsx", ADMINISTRATIVE_XLSX_URL)
    context.export_resource(path, XLSX, title="Administrative Debarments")
    wb = openpyxl.load_workbook(path, read_only=True)
    for row in h.parse_xlsx_sheet(context, wb.worksheets[0]):
        crawl_debarment(
            context, row, "Administratively Debarred Parties", "name", "date"
        )
