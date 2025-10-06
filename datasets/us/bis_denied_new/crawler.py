import csv

import openpyxl
import xlrd
from followthemoney.types import registry
from rigour.mime.types import XLS

from zavod import Context
from zavod import helpers as h

# Program key reused from 'us_trade_csl' for consistency
# Refers to the same BIS Denied Persons List program
PROGRAM_KEY = "US-BIS-DPL"


def parse_row(context: Context, row):
    entity = context.make("LegalEntity")
    effective_date = row.pop("effective_date")
    name = row.pop("name")
    country = row.pop("country")
    city = row.pop("city")

    entity.id = context.make_id(name, city, country)
    entity.add("name", name)
    entity.add("notes", row.pop("action"))
    entity.add("country", country)
    h.apply_date(entity, "modifiedAt", row.pop("last_update"))

    country_code = registry.country.clean(country)
    address = h.make_address(
        context,
        street=row.pop("street_address"),
        postal_code=row.pop("postal_code"),
        city=city,
        region=row.pop("state"),
        country_code=country_code,
    )
    h.copy_address(entity, address)

    citation = row.pop("fr_citation")
    # We don't link it to the website here, since it's included in the us_trade_csl
    # programs, which is linked to the website.
    sanction = h.make_sanction(context, entity, key=citation, program_key=PROGRAM_KEY)
    sanction.add("program", citation)
    h.apply_date(sanction, "startDate", effective_date)
    h.apply_date(sanction, "endDate", row.pop("expiration_date"))

    if h.is_active(sanction):
        entity.add("topics", "sanction")

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(
        row,
        [
            "counter",
            "standard_order",
            "type_of_denial",
            "name_and_address",
        ],
    )


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
            res = context.lookup("columns", field)
            if res is None:
                context.log.warning("Unknown column", column=field)
                norm_fieldnames.append(field.lower().replace(" ", "_"))
            norm_fieldnames.append(res.value)
        with open(path, "rt", encoding="utf-8-sig") as infh:
            reader = csv.DictReader(infh, fieldnames=norm_fieldnames)
            next(reader)  # Skip header row
            rows = list(reader)
    else:
        raise Exception(f"No known extension for {path}")

    context.export_resource(path, XLS, title=context.SOURCE_TITLE)

    for row in rows:
        parse_row(context, row)
