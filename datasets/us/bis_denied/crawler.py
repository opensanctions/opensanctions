import csv

import openpyxl
import xlrd
from rigour.mime.types import XLS

from zavod import Context
from zavod import helpers as h

# Program key reused from 'us_trade_csl' for consistency
# Refers to the same BIS Denied Persons List program
PROGRAM_KEY = "US-BIS-DPL"


def split_name_address(name_and_address: str):
    # Split only on the first comma
    parts = name_and_address.split(",", 1)
    name = parts[0].strip()
    address = parts[1].strip() if len(parts) > 1 else None
    return name, address


def parse_row(context: Context, row):
    entity = context.make("LegalEntity")
    name_and_address = row.pop("name_and_address")
    name, address = split_name_address(name_and_address)

    entity.id = context.make_id(name, address)
    entity.add("name", name)
    entity.add("notes", row.pop("type_of_denial"))
    address = h.make_address(context, full=address)
    h.copy_address(entity, address)

    citation = row.pop("appropriate_federal_register_citations")
    # We don't link it to the website here, since it's included in the us_trade_csl
    # programs, which is linked to the website.
    sanction = h.make_sanction(context, entity, key=citation, program_key=PROGRAM_KEY)
    sanction.add("program", citation)
    h.apply_date(sanction, "startDate", row.pop("effective_date"))
    h.apply_date(sanction, "endDate", row.pop("expiration_date"))

    if h.is_active(sanction):
        entity.add("topics", "sanction")

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def crawl(context: Context):
    doc = context.fetch_html(context.data_url, cache_days=1)
    urls = doc.xpath(".//a[contains(normalize-space(.), 'Export as CSV')]/@href")
    assert len(urls) == 1, "Expected exactly one URL"
    url = urls[0]
    path = context.fetch_resource("source.whoknows", url)

    rows = None
    if ".xls" in url:
        context.log.info("Reading as XLS", url=url)
        wb = xlrd.open_workbook(path)
        assert wb.sheet_names() == ["dpl", "Sheet2", "Sheet3"]
        path = path.rename(path.with_suffix(".xls"))
        context.export_resource(path, XLS, title=context.SOURCE_TITLE)
        rows = h.parse_xls_sheet(context, wb["dpl"])
    elif ".xlsx" in url:
        context.log.info("Reading as XLSX", url=url)
        workbook: openpyxl.Workbook = openpyxl.load_workbook(path, read_only=True)
        assert set(workbook.sheetnames) == ["dpl", "Sheet2", "Sheet3"]
        path = path.rename(path.with_suffix(".xlsx"))
        rows = h.parse_xlsx_sheet(path, context.get_lookup("columns"))
    elif ".csv" in url:
        context.log.info("Reading as CSV", url=url)
        path = path.rename(path.with_suffix(".csv"))
        with open(path, "rt", encoding="utf-8-sig") as infh:
            reader = csv.DictReader(infh)
            fieldnames = reader.fieldnames
        norm_fieldnames = []
        for field in fieldnames:
            field = field.lower().replace(" ", "_")
            field = context.lookup_value("columns", field, field)
            norm_fieldnames.append(field)
        with open(path, "rt", encoding="utf-8-sig") as infh:
            reader = csv.DictReader(infh, fieldnames=norm_fieldnames)
            next(reader)  # Skip header row
            rows = list(reader)
    else:
        raise Exception(f"No known extension for {path}")

    context.export_resource(path, XLS, title=context.SOURCE_TITLE)

    for row in rows:
        row = {(k.lower()): v for k, v in row.items()}
        parse_row(context, row)
