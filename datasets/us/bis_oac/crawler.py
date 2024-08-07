import re
from urllib.parse import urljoin
from typing import Dict
from zavod import Context
from zavod import helpers as h
from normality import collapse_spaces, stringify
from rigour.mime.types import XLSX
from openpyxl import load_workbook
from typing import List, Optional, Iterable


DATE_FORMAT = ["%Y-%m-%d"]  # 'YYYY-MM-DD' format
DATE_CLEAN = re.compile(r"\d{4}-\d{2}-\d{2}")


def extract_date(date_str: str) -> str:
    """Extract date from a string using regex."""
    match = re.search(DATE_CLEAN, date_str)
    if match:
        return match.group(0)
    return ""


def crawl_row(context: Context, row: Dict[str, str]):
    requester = row.pop("REQUESTER", "").strip()
    requesting_country = row.pop("REQUESTING COUNTRY", "").strip()
    # Extract and parse the date part only
    raw_date = row.pop("DATE LISTED", "").strip()
    date_part = extract_date(raw_date)
    date_listed = h.parse_date(date_part, DATE_FORMAT)[0]
    if not requester or not requesting_country or not date_listed:
        context.log.warning(
            "Missing requester, requesting country, or date listed", row=row
        )
        return

    entity = context.make("Company")
    entity.id = context.make_id(requester, date_listed)
    entity.add("name", requester)
    entity.add("country", requesting_country)
    entity.add("topics", "export.risk")
    sanction = h.make_sanction(context, entity)
    sanction.add("listingDate", date_listed)

    context.emit(entity, target=True)
    context.emit(sanction)


def crawl_xlsx(context: Context, path: str):
    wb = load_workbook(path, read_only=True)
    for sheet in wb.worksheets:
        headers = [
            collapse_spaces(str(c.value))
            for c in list(sheet.iter_rows(min_row=1, max_row=1))[0]
        ]

        for row in sheet.iter_rows(min_row=2, values_only=True):
            data = {headers[i]: stringify(row[i]) for i in range(len(headers))}
            crawl_row(context, data)


def fetch_excel_url(context: Context) -> str:
    params = {"_": context.data_time.date().isoformat()}
    doc = context.fetch_html(context.data_url, params=params)
    for link in doc.findall(".//a"):
        href = urljoin(context.data_url, link.get("href"))
        if href.endswith(".xls") or href.endswith(".xlsx"):
            return href
    raise ValueError("Could not find XLS file on the website")


def crawl(context: Context):
    context.log.info("Fetching data from the source")
    url = fetch_excel_url(context)
    path = context.fetch_resource(
        "source.xls", url
    )  # Even though its initial name is source.xls, actual file is in xlsx format
    actual_path = path.with_suffix(".xlsx")
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)
    crawl_xlsx(context, actual_path)
