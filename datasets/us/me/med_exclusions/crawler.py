from typing import Dict
import json
from rigour.mime.types import XLSX
from openpyxl import load_workbook

from zavod import Context, helpers as h


def crawl_item(row: Dict[str, str], context: Context):

    if first_name := row.pop("provider_first_name"):
        entity = context.make("Person")
        entity.id = context.make_id(
            first_name, row.get("provider_last_name"), row.get("provider_mi")
        )
        h.apply_name(
            entity,
            first_name=first_name,
            last_name=row.pop("provider_last_name"),
            middle_name=row.pop("provider_mi"),
        )
    else:
        entity = context.make("Company")
        entity.id = context.make_id(row.get("provider_last_name"))
        entity.add("name", row.pop("provider_last_name"))

    # Number of alias
    for i in [1, 2, 3, 4]:
        first_name = row.pop(f"alias_first_name_{i}")
        last_name = row.pop(f"alias_last_name_{i}")

        if first_name == "N/A":
            first_name = None
        if last_name == "N/A":
            last_name = None

        if first_name and last_name:
            entity.add("alias", first_name + last_name)
        elif first_name:
            entity.add("alias", first_name)
        elif last_name:
            entity.add("alias", last_name)

    entity.add("country", "us")
    entity.add("topics", "debarment")
    entity.add("sector", row.pop("provider_type"))

    sanction = h.make_sanction(context, entity)

    h.apply_date(sanction, "startDate", row.pop("state_exclusion_start_date"))

    context.emit(entity, target=True)
    context.emit(sanction)

    context.audit_data(row)


def crawl_excel_url(context: Context):
    response = context.fetch_response(context.data_url)
    txt = response.text
    month = json.loads(
        txt[txt.find("WPQ2ListData") + 15 : txt.find("WPQ2SchemaData") - 5]
    )["Row"][0]["FileLeafRef"]
    return f"https://mainecare.maine.gov/PrvExclRpt/{month}/PI0008-PM%20Monthly%20Exclusion%20Report.xlsx"


def crawl(context: Context) -> None:
    # First we find the link to the excel file
    excel_url = crawl_excel_url(context)
    path = context.fetch_resource("list.xlsx", excel_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)

    for item in h.parse_xlsx_sheet(context, wb.active, skiprows=26):
        crawl_item(item, context)
