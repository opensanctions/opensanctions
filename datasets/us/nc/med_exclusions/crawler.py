from typing import Dict
from rigour.mime.types import XLSX
from openpyxl import load_workbook

from zavod import Context, helpers as h


def crawl_item(row: Dict[str, str], context: Context):

    entity = context.make("LegalEntity")
    entity.id = context.make_id(
        row.get("excluded_entity"), row.get("npi_atypical_id_excluded")
    )
    entity.add("name", row.pop("excluded_entity"))
    entity.add("npiCode", row.pop("npi_atypical_id_excluded"))
    entity.add("topics", "debarment")
    address = h.make_address(
        context,
        state=row.pop("state"),
        city=row.pop("city"),
        postal_code=row.pop("zip_code"),
        country="United States of America",
    )
    h.copy_address(entity, address)
    entity.add("country", "us")

    sanction = h.make_sanction(context, entity)
    sanction.add(
        "startDate",
        h.parse_date(row.pop("effective_date"), formats=["%m/%d/%Y"]),
    )
    sanction.add("reason", row.pop("reason_for_exclusion"))

    owner = context.make("Person")
    owner.id = context.make_id(row.get("ownership"))
    owner.add("name", row.pop("ownership"))

    ownership = context.make("Ownership")
    ownership.id = context.make_id(owner.id, entity.id)

    ownership.add("asset", entity)
    ownership.add("owner", owner)

    context.emit(entity, target=True)
    context.emit(sanction)
    context.emit(owner)
    context.emit(ownership)

    context.audit_data(row)


def crawl_excel_url(context: Context):
    doc = context.fetch_html(context.data_url)
    doc.make_links_absolute(context.data_url)
    return doc.xpath("//*[text() = 'State Excluded Provider List']")[0].get("href")


def crawl(context: Context) -> None:

    excel_url = crawl_excel_url(context)

    path = context.fetch_resource("list.xlsx", excel_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)

    for item in h.parse_xlsx_sheet(context, wb.active, skiprows=6):
        crawl_item(item, context)
