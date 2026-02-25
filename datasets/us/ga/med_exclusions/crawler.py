from typing import Dict

from openpyxl import load_workbook

from rigour.mime.types import XLSX
from zavod import Context, helpers as h


def crawl_item(row: Dict[str, str | None], context: Context):
    first_name = row.pop("first_name")
    last_name = row.pop("last_name")
    # TODO: clean up AKAs in middle_name
    middle_name = row.pop("middle_name")
    business_name = row.pop("business_name")
    npi = row.pop("npi")

    if not first_name and not last_name and not business_name:
        return

    has_person_name = first_name or last_name
    # Only business name -> Company
    if business_name and not has_person_name:
        entity = context.make("Company")
        entity.id = context.make_id(business_name, npi)
        entity.add("name", business_name)
    # Person name (with optional business name as d.b.a. alias)
    else:
        entity = context.make("Person")
        entity.id = context.make_id(first_name, last_name, npi)
        h.apply_name(
            entity, first_name=first_name, middle_name=middle_name, last_name=last_name
        )
        if business_name:
            entity.add("alias", business_name)

    entity.add("npiCode", npi)
    entity.add("topics", "debarment")
    entity.add("country", "us")
    entity.add("sector", row.pop("general"))

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", row.pop("sancdate"))

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row, ignore=["state"])


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, absolute_links=True)
    excel_url = h.xpath_string(
        doc, ".//a[contains(@data-text, 'List of Excluded Individuals')]/@href"
    )
    path = context.fetch_resource("list.xlsx", excel_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)
    for item in h.parse_xlsx_sheet(context, wb["Sheet1"], skiprows=2):
        crawl_item(item, context)
