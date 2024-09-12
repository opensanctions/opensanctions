from typing import Dict
from rigour.mime.types import XLSX
from openpyxl import load_workbook

from zavod import Context, helpers as h


def crawl_item(row: Dict[str, str], context: Context):

    if not row.get("entity_name"):
        return

    entity_name = row.pop("entity_name")
    entity = context.make("LegalEntity")
    entity.id = context.make_id(entity_name, row.get("npi"))

    if row.get("first_name"):
        person = context.make("Person")
        person.id = context.make_id(
            row.get("first_name"), row.get("middle_name"), row.get("last_name")
        )

        person.add("topics", "debarment")

        link = context.make("UnknownLink")
        link.id = context.make_id(person.id, entity.id)
        link.add("object", entity)
        link.add("subject", person)

        person_sanction = h.make_sanction(context, person)
        person_sanction.add(
            "startDate",
            h.parse_date(row.get("sanction_date1"), formats=["%Y-%m-%d", "%m/%d/%Y"]),
        )
        person_sanction.add("publisher", row.get("sanction_source"))
        person_sanction.add("reason", row.get("reason"))

        context.emit(person, target=True)
        context.emit(person_sanction)
        context.emit(link)

    entity.add("name", entity_name)

    entity.add("npiCode", row.pop("npi"))

    entity.add("topics", "debarment")
    entity.add("sector", row.pop("provider_category"))

    if row.get("license"):
        entity.add("description", "License number: " + row.pop("license"))

    sanction = h.make_sanction(context, entity)
    sanction.add(
        "startDate",
        h.parse_date(row.pop("sanction_date1"), formats=["%Y-%m-%d", "%m/%d/%Y"]),
    )
    sanction.add("publisher", row.pop("sanction_source"))
    sanction.add("reason", row.pop("reason"))

    context.emit(entity, target=True)
    context.emit(sanction)

    context.audit_data(row, ignore=["city", "sanction_date2"])


def crawl_excel_url(context: Context):
    doc = context.fetch_html(context.data_url)
    doc.make_links_absolute(context.data_url)
    return doc.xpath("//*[text()='List of Sanctioned Providers (XLSX)']/../..")[0].get(
        "href"
    )


def crawl(context: Context) -> None:
    # First we find the link to the excel file
    excel_url = crawl_excel_url(context)
    path = context.fetch_resource("list.xlsx", excel_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)

    for item in h.parse_xlsx_sheet(context, wb.active, skiprows=1):
        crawl_item(item, context)
