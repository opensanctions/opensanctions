from typing import Dict
from rigour.mime.types import XLSX
from openpyxl import load_workbook

from zavod import Context, helpers as h


def crawl_item(row: Dict[str, str], context: Context):

    if not row.get("name"):
        return

    names = row.pop("name").split("/")
    termination_date = row.pop("termination_date")
    comments = row.pop("comments")
    entity = context.make("LegalEntity")
    entity.id = context.make_id(names, row.get("npi"))

    for name in names:
        entity.add("name", name)

    if row.get("npi") != "N/A":
        entity.add("npiCode", row.pop("npi"))
    else:
        row.pop("npi")

    entity.add("topics", "debarment")
    entity.add("sector", row.pop("provider_type"))
    entity.add("description", row.pop("kmap_provider"))

    sanction = h.make_sanction(context, entity)
    sanction.add("startDate", termination_date)
    sanction.add("summary", comments)

    context.emit(entity, target=True)
    context.emit(sanction)

    business_name = row.pop("d_b_a_business_name")

    if business_name is not None:
        company = context.make("Company")
        company.id = context.make_id(business_name)
        company.add("name", business_name)
        company.add("topics", "debarment")

        link = context.make("UnknownLink")
        link.id = context.make_id(names, business_name)
        link.add("object", entity)
        link.add("subject", company)
        link.add("role", "d/b/a")

        company_sanction = h.make_sanction(context, company)
        company_sanction.add("startDate", termination_date)
        company_sanction.add("summary", comments)

        context.emit(company, target=True)
        context.emit(company_sanction)
        context.emit(link)

    context.audit_data(row)


def crawl_excel_url(context: Context):
    doc = context.fetch_html(context.data_url)
    doc.make_links_absolute(context.data_url)
    return doc.xpath("//*[text()='Termination List (XLSX)']")[0].get("href")


def crawl(context: Context) -> None:
    # First we find the link to the excel file
    excel_url = crawl_excel_url(context)
    path = context.fetch_resource("list.xlsx", excel_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)

    for item in h.parse_xlsx_sheet(context, wb.active, skiprows=1):
        crawl_item(item, context)
