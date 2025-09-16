from typing import Dict
from rigour.mime.types import XLSX
from openpyxl import load_workbook

from zavod import Context, helpers as h


def crawl_item(row: Dict[str, str], context: Context):

    name = row.pop("provider_name")

    if not name:
        return

    if "for purposes of" in name.lower():
        return

    entity = context.make("LegalEntity")
    entity.id = context.make_id(name, row.get("national_provider_identification_npi"))
    entity.add("name", name)
    entity.add("npiCode", row.pop("national_provider_identification_npi"))
    entity.add("address", row.pop("service_location_address").split("\n"))
    entity.add("country", "us")

    sanction = h.make_sanction(context, entity)
    sanction.add("startDate", row.pop("termination_effective_date"))

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def crawl_excel_url(context: Context):
    doc = context.fetch_html(context.data_url, absolute_links=True)
    return doc.xpath("//*[text()='Terminated providers']")[0].get("href")


def crawl(context: Context) -> None:
    # First we find the link to the excel file
    excel_url = crawl_excel_url(context)
    path = context.fetch_resource("list.xlsx", excel_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)

    for item in h.parse_xlsx_sheet(context, wb.active, skiprows=1):
        crawl_item(item, context)
