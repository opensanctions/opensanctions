from typing import Dict
from rigour.mime.types import XLSX
from openpyxl import load_workbook

from zavod import Context, helpers as h


def crawl_item(row: Dict[str, str], context: Context):

    first_name = row.pop("first_name")
    last_name = row.pop("last_name")
    business_name = row.pop("business_name")

    if not first_name and not last_name and not business_name:
        return

    if business_name:
        entity = context.make("Company")
        entity.id = context.make_id(business_name, row.get("npi"))
        entity.add("name", business_name)
    else:
        entity = context.make("Person")
        entity.id = context.make_id(first_name, last_name, row.get("npi"))
        h.apply_name(entity, first_name=first_name, last_name=last_name)

    if row.get("npi"):
        entity.add("npiCode", row.pop("npi"))

    entity.add("topics", "debarment")
    entity.add("country", "us")
    entity.add("sector", row.pop("general"))

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", row.pop("sancdate"))

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row, ignore=["state"])


def crawl_excel_url(context: Context):
    doc = context.fetch_html(context.data_url)
    doc.make_links_absolute(context.data_url)
    return doc.find(".//a[@data-text='List of Excluded Individuals']").get("href")


def crawl(context: Context) -> None:
    # First we find the link to the excel file
    excel_url = crawl_excel_url(context)
    path = context.fetch_resource("list.xlsx", excel_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)

    for item in h.parse_xlsx_sheet(context, wb["Sheet1"], skiprows=2):
        crawl_item(item, context)
