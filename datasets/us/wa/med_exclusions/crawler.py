from typing import Dict
from rigour.mime.types import XLSX
from openpyxl import load_workbook

from zavod import Context, helpers as h


def crawl_item(row: Dict[str, str], context: Context):

    if len(row.get("name").split("\n")) == 1:
        name = row.pop("name")
        aliases = []
    else:
        name_parts = row.pop("name").split("\n")
        name = name_parts[0]
        aliases = name_parts[1:]

    entity = context.make("LegalEntity")
    entity.id = context.make_id(name, row.get("npi_or_p1"))
    entity.add("name", name)
    if row.get("npi_or_p1").startswith("P1"):
        entity.add("description", "P1: " + row.pop("npi_or_p1"))
    else:
        entity.add("npiCode", row.pop("npi_or_p1"))
    entity.add("alias", aliases)
    entity.add("topics", "debarment")
    entity.add("idNumber", row.pop("license"))
    entity.add("country", "us")

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", row.pop("date_of_exclusion"))
    sanction.add("provisions", row.pop("action"))

    context.emit(entity, target=True)
    context.emit(sanction)

    context.audit_data(row)


def crawl_excel_url(context: Context):
    doc = context.fetch_html(context.data_url)
    doc.make_links_absolute(context.data_url)
    return doc.xpath(".//a[contains(text(), 'the HCA Medicaid providers listing')]")[
        0
    ].get("href")


def crawl(context: Context) -> None:
    path = context.fetch_resource("list.xlsx", crawl_excel_url(context))
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)

    for item in h.parse_xlsx_sheet(context, wb.active, skiprows=3):
        crawl_item(item, context)
