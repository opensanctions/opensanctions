from typing import Dict
from rigour.mime.types import XLSX
from openpyxl import load_workbook

from zavod import Context, helpers as h


def crawl_item(row: Dict[str, str], context: Context):

    if not row.get("provider_name"):
        return

    provider_name = row.pop("provider_name")
    entity = context.make("LegalEntity")
    entity.id = context.make_id(provider_name, row.get("n_p_i"))
    entity.add("name", provider_name)

    if row.get("n_p_i") != "N/A":
        entity.add("npiCode", row.pop("n_p_i"))
    else:
        row.pop("n_p_i")

    if row.get("business_nameand_address") != "N/A":
        entity.add("address", row.pop("business_nameand_address"))
    else:
        row.pop("business_nameand_address")

    entity.add("topics", "debarment")
    entity.add("sector", row.pop("provider_type"))
    if row.get("medicaid_provider_id"):
        entity.add("description", "Medicaid Provider ID: "+row.pop("medicaid_provider_id"))
    if row.get("medicareprovidernumber"):
        entity.add("description", "Medicare Provider Number: "+row.pop("medicareprovidernumber"))
    sanction = h.make_sanction(context, entity)
    termination_date = row.pop("exclusiondate").replace("Termination ", "")
    sanction.add(
        "startDate", h.parse_date(termination_date, formats=["%m/%d/%Y", "%m/%d/%y"])
    )
    sanction.add("reason", row.pop("reason_for_exclusion"))

    context.emit(entity, target=True)
    context.emit(sanction)

    context.audit_data(
        row,
        ignore=[
            "providerverification",
            "state",
        ],
    )


def crawl_excel_url(context: Context):
    doc = context.fetch_html(context.data_url)
    doc.make_links_absolute(context.data_url)
    return doc.xpath(
        "//a[@title='ND Medicaid Provider Exclusion List'][substring(@href, string-length(@href) - 4) = '.xlsx']/@href"
    )[0]


def crawl(context: Context) -> None:
    # First we find the link to the excel file
    excel_url = crawl_excel_url(context)
    path = context.fetch_resource("list.xlsx", excel_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)

    for item in h.parse_xlsx_sheet(context, wb.active, skiprows=0):
        crawl_item(item, context)
