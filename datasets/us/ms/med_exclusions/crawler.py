from typing import Dict
from rigour.mime.types import XLSX
from openpyxl import load_workbook

from zavod import Context, helpers as h


def crawl_item(row: Dict[str, str], context: Context):
    provider_name = row.pop("provider_name")
    dob = row.pop("date_of_birth")
    npi = row.pop("npi")

    if dob:
        schema = "Person"
    else:
        schema = "LegalEntity"

    entity = context.make(schema)
    entity.id = context.make_id(provider_name, row.get("npi"))

    entity.add("name", provider_name)
    entity.add("country", "us")
    entity.add("sector", row.pop("provider_type_specialty"))
    entity.add("address", row.pop("provider_address"))
    if entity.schema.name == "Person":
        h.apply_date(entity, "birthDate", dob)
    if npi:
        entity.add("npiCode", npi.split("\n"))

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", row.pop("termination_effective_date"))
    sanction.add("reason", row.pop("termination_reason"))

    end_date = row.pop("exclusion_period").split("-")[1].strip()

    if end_date not in ["Indefinite", "indefinite"]:
        h.apply_date(sanction, "endDate", end_date)
    else:
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
