from typing import List, Optional
from normality import slugify
import openpyxl
from zavod import Context
from pantomime.types import XLSX

STATUTORY_XLSX_URL = "https://www.pmddtc.state.gov/sys_attachment.do?sys_id=27c46b251baf29102b6ca932f54bcb20"
ADMINISTRATIVE_XLSX_URL = "https://www.pmddtc.state.gov/sys_attachment.do?sys_id=9f8bbc2f1b8f29d0c6c3866ae54bcbdb"


def sheet_to_dicts(sheet):
    headers: Optional[List[str]] = None
    for row in sheet.rows:
        cells = [c.value for c in row]
        if headers is None:
            headers = [slugify(h) for h in cells]
            continue
        row = dict(zip(headers, cells))
        yield row


def crawl_debarment(context, row):
    print(row)


def crawl(context: Context):
    path = context.fetch_resource("statutory.xlsx", STATUTORY_XLSX_URL)
    context.export_resource(path, XLSX, title="Statutory Debarments")
    rows = sheet_to_dicts(openpyxl.load_workbook(path, read_only=True).worksheets[0])
    for row in rows:
        crawl_debarment(context, row)

    path = context.fetch_resource("administrative.xlsx", ADMINISTRATIVE_XLSX_URL)
    context.export_resource(path, XLSX, title="Administrative Debarments")
    rows = sheet_to_dicts(openpyxl.load_workbook(path, read_only=True).worksheets[0])
    for row in rows:
        crawl_debarment(context, row)
