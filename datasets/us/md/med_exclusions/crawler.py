from typing import Dict
from rigour.mime.types import XLSX
from openpyxl import load_workbook

from zavod import Context, helpers as h


def crawl_item(row: Dict[str, str], context: Context):

    if not row.get("first_name"):
        entity = context.make("Company")
        entity.id = context.make_id(row.get("last_name_organization"))
        entity.add("name", row.pop("last_name_organization"))
    else:
        entity = context.make("Person")
        entity.id = context.make_id(
            row.get("last_name_organization"), row.get("first_name")
        )
        h.apply_name(
            entity,
            first_name=row.pop("first_name"),
            last_name=row.pop("last_name_organization"),
        )

    entity.add("npiCode", h.multi_split(row.pop("npi"), ","))
    entity.add("sector", row.pop("type_of_entity_profession"))
    entity.add("topics", "debarment")
    entity.add("country", "us")

    if row.get("license_no"):
        entity.add("description", "License No: " + row.pop("license_no"))

    street_address = row.pop("address") or ""
    city_state_zip = row.pop("city_state_zip") or ""
    if street_address or city_state_zip:
        entity.add("address", f"{street_address}, {city_state_zip}")

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", row.pop("termination_sanction_date"))
    sanction.add("description", "Sanction Type: " + row.pop("sanction_type"))

    context.emit(entity, target=True)
    context.emit(sanction)

    context.audit_data(row)


def crawl_excel_url(context: Context):
    doc = context.fetch_html(context.data_url)
    doc.make_links_absolute(context.data_url)
    return doc.xpath(
        ".//a[contains(text(), 'Maryland Medicaid Sanctioned Provider List')]"
    )[0].get("href")


def crawl(context: Context) -> None:
    path = context.fetch_resource("list.xlsx", crawl_excel_url(context))
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)

    for item in h.parse_xlsx_sheet(context, wb.active):
        # it means the table has ended and the rest is just the sanction types
        if item.get("last_name_organization") == "Sanction Type":
            return
        crawl_item(item, context)
