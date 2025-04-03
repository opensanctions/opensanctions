from typing import Dict
from rigour.mime.types import XLSX
from openpyxl import load_workbook

from zavod import Context, helpers as h


def crawl_item(row: Dict[str, str], context: Context):

    if not row.get("provider_name"):
        return

    provider_name = row.pop("provider_name")

    if row.get("date_of_birth"):
        schema = "Person"
    else:
        schema = "LegalEntity"

    entity = context.make(schema)
    entity.id = context.make_id(provider_name, row.get("npi"))

    entity.add("name", provider_name)

    if row.get("date_of_birth"):
        entity.add("birthDate", row.pop("date_of_birth"))

    if row.get("npi"):
        entity.add("npiCode", row.pop("npi").split("\n"))
    else:
        row.pop("npi")

    entity.add("country", "us")
    entity.add("sector", row.pop("provider_type_specialty"))
    entity.add("address", row.pop("provider_address"))

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", row.pop("termination_effective_date"))
    sanction.add("reason", row.pop("termination_reason"))

    end_date = row.pop("exclusion_period")
    # When in the ISO format (e.g. 2025-03-07) and mirrors the start date
    # should be set to 'Indefinite'
    if len(h.multi_split(end_date, ["-"])) > 2:
        end_date_lookup = context.lookup("end_date", end_date)
        if not end_date_lookup:
            context.log.warning("End date not found in lookup", end_date=end_date)
    # Most common case: 'March 20, 2023 - Indefinite'
    elif len(h.multi_split(end_date, ["-"])) == 2:
        end_date = end_date.split("-")[1].strip()
    else:
        context.log.warning("Check the splitting logic for end_date", end_date=end_date)

    if end_date not in ["Indefinite", "indefinite"]:
        h.apply_date(sanction, "endDate", end_date)
        is_debarred = False
    else:
        is_debarred = True

    if is_debarred:
        entity.add("topics", "debarment")

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row, ignore=["sanction_type", "column_0"])


def crawl_excel_url(context: Context):
    doc = context.fetch_html(context.data_url)
    doc.make_links_absolute(context.data_url)
    return doc.xpath("//a[contains(text(), 'Sanctioned Provider List')]/@href")[0]


def crawl(context: Context) -> None:
    # First we find the link to the excel file
    excel_url = crawl_excel_url(context)
    path = context.fetch_resource("list.xlsx", excel_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)

    for item in h.parse_xlsx_sheet(context, wb.active, skiprows=8):
        crawl_item(item, context)
