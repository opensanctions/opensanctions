from typing import Dict
from openpyxl import load_workbook

from zavod import Context, helpers as h


def crawl_item(row: Dict[str, str], context: Context):

    name = row.pop("provider_name")

    if not name:
        return

    entity = context.make("LegalEntity")
    entity.id = context.make_id(name, row.get("national_provider_identifier_npi"))
    entity.add("name", name)
    entity.add("npiCode", row.pop("national_provider_identifier_npi"))
    entity.add("sector", row.pop("provider_type"))
    entity.add("country", "us")
    entity.add("idNumber", row.pop("unique_id"))

    entity.add("topics", "debarment")

    sanction = h.make_sanction(context, entity)
    sanction.add("startDate", row.pop("suspension_exclusion_effective_date"))
    sanction.add("reason", row.pop("suspension_exclusion_reason"))

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def crawl_excel_url(context: Context):
    doc = context.fetch_html(context.data_url)
    doc.make_links_absolute(context.data_url)
    return doc.xpath("//*[contains(text(), 'XLSX')]/../@href")[0]


def crawl(context: Context) -> None:
    # First we find the link to the excel file
    excel_url = crawl_excel_url(context)

    path = context.fetch_resource("source.xlsx", excel_url)
    context.export_resource(path, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)

    for item in h.parse_xlsx_sheet(context, wb["Exclusions List"]):
        crawl_item(item, context)
