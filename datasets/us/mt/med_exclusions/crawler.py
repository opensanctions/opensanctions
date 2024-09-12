from typing import Dict
from rigour.mime.types import XLSX
from openpyxl import load_workbook

from zavod import Context, helpers as h


def crawl_item(row: Dict[str, str], context: Context):

    if ", " not in row.get("terminated_excluded_provider_s"):
        entity = context.make("Company")
        entity.id = context.make_id(row.get("terminated_excluded_provider_s"))
        entity.add("name", row.pop("terminated_excluded_provider_s"))
    else:
        entity = context.make("Person")
        entity.id = context.make_id(row.get("terminated_excluded_provider_s"))
        last_name, first_name = row.pop("terminated_excluded_provider_s").split(", ")
        entity.add("firstName", first_name)
        entity.add("lastName", last_name)

    entity.add("topics", "debarment")
    entity.add("sector", row.pop("healthcare_profession"))
    entity.add("country", "us")

    sanction = h.make_sanction(context, entity)
    sanction.add(
        "startDate",
        h.parse_date(row.pop("effective_date"), formats=["%m/%d/%Y"]),
    )

    context.emit(entity, target=True)
    context.emit(sanction)

    context.audit_data(row, ignore=["column_3", "alias"])


def crawl_excel_url(context: Context):
    doc = context.fetch_html(context.data_url)
    doc.make_links_absolute(context.data_url)
    return doc.xpath(
        ".//a[text()='Download Excluded or Terminated Provider list in Excel']"
    )[0].get("href")


def crawl(context: Context) -> None:

    excel_url = crawl_excel_url(context)

    path = context.fetch_resource("list.xlsx", excel_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)

    current_item = {}
    for item in h.parse_xlsx_sheet(context, wb.active):
        if item.get("terminated_excluded_provider_s").startswith("a.k.a."):
            current_item["alias"].append(item.get("terminated_excluded_provider_s"))
        else:
            current_item = item
            current_item["alias"] = []
            crawl_item(current_item, context)
