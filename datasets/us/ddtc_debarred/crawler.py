from typing import Any, Dict, List, Optional
from normality import slugify
import openpyxl
from zavod import Context
from pantomime.types import XLSX
from zavod import helpers as h

STATUTORY_XLSX_URL = "https://www.pmddtc.state.gov/sys_attachment.do?sys_id=27c46b251baf29102b6ca932f54bcb20"
ADMINISTRATIVE_XLSX_URL = "https://www.pmddtc.state.gov/sys_attachment.do?sys_id=9f8bbc2f1b8f29d0c6c3866ae54bcbdb"
STATUTORY_PROGRAM = "Statutory debarment pursuant to §38(g)(4) of the AECA and §127.7 of the International Traffic in Arms Regulations (ITAR)"
ADMINISTRATIVE_PROGRAM = "Administrative debarment for violations of the AECA/ITAR, as specified in 22 CFR §127.7(a)"

FORMATS = ["%B, %Y", "%B %Y"]


def sheet_to_dicts(sheet):
    headers: Optional[List[str]] = None
    for row in sheet.rows:
        cells = [c.value for c in row]
        if headers is None:
            headers = [slugify(h) for h in cells]
            continue
        row = dict(zip(headers, cells))
        yield row


def split_names(name: str) -> (str, List[str]):
    parts = name.split("(a.k.a. ", 1)
    main_name = parts[0]
    alias_list = []
    if len(parts) > 1:
        aliases = parts[1]
        aliases = aliases.replace(")", "")
        alias_list = aliases.split("; ")
    return main_name, alias_list


def crawl_debarment(
    context: Context,
    row: Dict[str, Any],
    program: str,
    name_field: str,
    notice_date_field: str,
    birth_date_field=None,
):
    date_of_birth = row.pop(birth_date_field, None)
    if date_of_birth:
        schema = "Person"
    else:
        schema = "LegalEntity"

    entity = context.make(schema)
    name, aliases = split_names(row.pop(name_field))
    entity.id = context.make_slug(name, date_of_birth, strict=False)
    entity.add("name", name)
    entity.add("alias", aliases)
    if schema == "Person":
        entity.add("birthDate", h.parse_date(date_of_birth, FORMATS))
    entity.add("topics", "debarment")

    sanction = h.make_sanction(context, entity)
    sanction.add("program", program)
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

    context.emit(entity, target=True)
    context.emit(sanction)

    context.audit_data(row, ["charging-letter", "debarment-order"])


def crawl(context: Context):
    path = context.fetch_resource("statutory.xlsx", STATUTORY_XLSX_URL)
    context.export_resource(path, XLSX, title="Statutory Debarments")
    rows = sheet_to_dicts(openpyxl.load_workbook(path, read_only=True).worksheets[0])
    for row in rows:
        crawl_debarment(
            context,
            row,
            STATUTORY_PROGRAM,
            "party-name",
            "notice-date",
            "date-of-birth",
        )

    path = context.fetch_resource("administrative.xlsx", ADMINISTRATIVE_XLSX_URL)
    context.export_resource(path, XLSX, title="Administrative Debarments")
    rows = sheet_to_dicts(openpyxl.load_workbook(path, read_only=True).worksheets[0])
    for row in rows:
        crawl_debarment(context, row, ADMINISTRATIVE_PROGRAM, "name", "date")
