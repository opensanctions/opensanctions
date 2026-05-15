from normality import slugify
import openpyxl
from rigour.mime.types import XLSX
from typing import Any, Dict, List, Optional, Tuple

from zavod import Context
from zavod import helpers as h

STATUTORY_XLSX_URL = "https://www.pmddtc.state.gov/sys_attachment.do?sys_id=27c46b251baf29102b6ca932f54bcb20"
ADMINISTRATIVE_XLSX_URL = "https://www.pmddtc.state.gov/sys_attachment.do?sys_id=9f8bbc2f1b8f29d0c6c3866ae54bcbdb"
US_DDTC_SD = "US-DDTC-SD"
US_DDTC_AD = "US-DDTC-AD"


def sheet_to_dicts(sheet):
    headers: Optional[List[str]] = None
    for row in sheet.rows:
        cells = [c.value for c in row]
        if headers is None:
            headers = [slugify(h) or f"column_{i}" for i, h in enumerate(cells)]
            continue
        row = dict(zip(headers, cells))
        yield row


def split_names(name: str) -> Tuple[str, List[str]]:
    parts = name.split("(a.k.a. ", 1)
    main_name = parts[0]
    alias_list: List[str] = []
    if len(parts) > 1:
        aliases = parts[1]
        aliases = aliases.replace(")", "")
        alias_list = aliases.split("; ")
    return main_name, alias_list


def crawl_debarment(
    context: Context,
    row: Dict[str, Any],
    program_key: str,
    name_field: str,
    notice_date_field: str,
):
    date_of_birth = row.pop("date-of-birth", None)
    if date_of_birth:
        schema = "Person"
    else:
        schema = "LegalEntity"

    entity = context.make(schema)
    raw_name = row.pop(name_field)
    name, aliases = split_names(raw_name)

    prev_dob = date_of_birth
    if isinstance(date_of_birth, str):
        prev_dob = prev_dob.strip()
    old_id = context.make_slug(name, prev_dob, strict=False)
    entity.id = context.make_id(raw_name, date_of_birth)
    context.rekey(old_id, entity.id)

    entity.add("name", name)
    entity.add("alias", aliases)
    entity.add("country", "us")
    if schema == "Person":
        h.apply_date(entity, "birthDate", date_of_birth)
    entity.add("topics", "debarment")

    original = h.Names(name=raw_name)
    suggested = h.Names()
    suggested.add("name", name)
    for alias in aliases:
        suggested.add("alias", alias)
    is_irregular, _ = h.check_names_regularity(entity, original)
    h.review_names(
        context,
        entity,
        original=original,
        suggested=suggested,
        is_irregular=is_irregular,
    )

    sanction = h.make_sanction(context, entity, program_key=program_key)
    sanction.add("listingDate", row.pop(notice_date_field).isoformat()[:10])
    sanction.add("listingDate", row.pop("corrected-notice-date", None))
    sanction.add(
        "description",
        "Federal register notice: " + row.pop("federal-register-notice"),
    )
    corrected_notice_number = row.pop("corrected-notice", None)
    if corrected_notice_number:
        sanction.add(
            "description",
            "Corrected federal register notice: " + corrected_notice_number,
        )

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row, ["charging-letter", "debarment-order"])


def crawl(context: Context):
    path = context.fetch_resource("statutory.xlsx", STATUTORY_XLSX_URL)
    context.export_resource(path, XLSX, title="Statutory Debarments")
    rows = sheet_to_dicts(openpyxl.load_workbook(path, read_only=True).worksheets[0])
    for row in rows:
        crawl_debarment(context, row, US_DDTC_SD, "party-name", "notice-date")

    path = context.fetch_resource("administrative.xlsx", ADMINISTRATIVE_XLSX_URL)
    context.export_resource(path, XLSX, title="Administrative Debarments")
    rows = sheet_to_dicts(openpyxl.load_workbook(path, read_only=True).worksheets[0])
    for row in rows:
        crawl_debarment(context, row, US_DDTC_AD, "name", "date")
