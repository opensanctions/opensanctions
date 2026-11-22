from typing import Any

from python_calamine import CalamineWorkbook
from rigour.mime.types import XLSX

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
    statutory: bool,
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
    # Either column of a corrected row may cite the correcting notice (e.g. 57 FR
    # 43768 corrects the older 57 FR 37184, while 85 FR 39967 corrects the older
    # 85 FR 30783), so the earlier notice is the listing.
    notice_dates = [row.pop(notice_date_field)]
    corrected_notice_date = row.pop("corrected_notice_date", None)
    if corrected_notice_date:
        notice_dates.append(corrected_notice_date)
    notice_dates.sort()
    h.apply_date(sanction, "listingDate", notice_dates[0])
    if len(notice_dates) > 1:
        h.apply_date(sanction, "modifiedAt", notice_dates[-1])
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

    if statutory:
        # Statutory notices up to 2010 took effect on each person's conviction
        # date, which the list doesn't give; from 76 FR 70805 on, they take
        # effect on the notice date. Corrections are dated by their own notice.
        h.finalize_sanction(
            context,
            sanction,
            effective_on_listing=notice_dates[0] >= "2011-11-15",
            amendments_dated=True,
        )
    else:
        # The administrative list's date is the order's effective date on some
        # rows and the notice's publication date on others, so it's no start date.
        h.finalize_sanction(context, sanction)

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row, ["charging_letter", "debarment_order"])


def crawl(context: Context) -> None:
    path = context.fetch_resource("statutory.xlsx", STATUTORY_XLSX_URL)
    context.export_resource(path, XLSX, title="Statutory Debarments")
    workbook = CalamineWorkbook.from_path(path)
    assert len(workbook.sheet_names) == 1, workbook.sheet_names
    for row in h.parse_xlsx_sheet(context, workbook.get_sheet_by_index(0)):
        crawl_debarment(
            context,
            row,
            "Statutorily Debarred Parties",
            "party_name",
            "notice_date",
            statutory=True,
        )

    path = context.fetch_resource("administrative.xlsx", ADMINISTRATIVE_XLSX_URL)
    context.export_resource(path, XLSX, title="Administrative Debarments")
    workbook = CalamineWorkbook.from_path(path)
    assert len(workbook.sheet_names) == 1, workbook.sheet_names
    for row in h.parse_xlsx_sheet(context, workbook.get_sheet_by_index(0)):
        crawl_debarment(
            context,
            row,
            "Administratively Debarred Parties",
            "name",
            "date",
            statutory=False,
        )
