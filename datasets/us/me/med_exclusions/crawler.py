from typing import Dict
import json
from rigour.mime.types import XLSX
from openpyxl import load_workbook

from zavod import Context, helpers as h


def crawl_item(row: Dict[str, str], context: Context):

    if first_name := row.pop("provider_first_name"):
        last_name = row.pop("provider_last_name")
        middle_initial = row.pop("provider_mi")

        entity = context.make("Person")
        entity.id = context.make_id(first_name, last_name, middle_initial)
        h.apply_name(
            entity,
            first_name=first_name,
            last_name=last_name,
            middle_name=middle_initial,
        )
        entity.add("position", row.pop("provider_type"))
    else:
        last_name = row.pop("provider_last_name")
        entity = context.make("Company")
        entity.id = context.make_id(last_name)
        entity.add("name", last_name)
        entity.add("sector", row.pop("provider_type"))

    # Number of alias
    for i in [1, 2, 3, 4]:

        alias_first_name = row.pop(f"alias_first_name_{i}")
        alias_last_name = row.pop(f"alias_last_name_{i}")

        if not alias_first_name and not alias_last_name:
            continue

        # If the entity is a company and there is an alias, we consider it as an unknown link
        if entity.schema.name == "Company":
            person = context.make("Person")
            person.id = context.make_id(alias_first_name, alias_last_name)
            h.apply_name(person, first_name=alias_first_name, last_name=alias_last_name)
            person.add("country", "us")

            link = context.make("UnknownLink")
            link.id = context.make_id(person.id, entity.id)
            link.add("object", entity)
            link.add("subject", person)

            context.emit(link)
            context.emit(person)

        else:
            h.apply_name(
                entity,
                first_name=alias_first_name,
                last_name=alias_last_name,
                alias=True,
            )

    entity.add("country", "us")
    entity.add("topics", "debarment")

    sanction = h.make_sanction(context, entity)

    h.apply_date(sanction, "startDate", row.pop("state_exclusion_start_date"))

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def crawl_excel_url(context: Context):
    response = context.fetch_response(context.data_url)
    txt = response.text
    # Parse out the table data JSON embedded in the HTML
    table_data = json.loads(
        txt[txt.find("WPQ2ListData") + 15 : txt.find("WPQ2SchemaData") - 5]
    )
    # Assert that the table is in descending date order (using ID as proxy for date)
    last_id = None
    for row in table_data["Row"]:
        assert last_id is None or last_id > row["ID"], last_id
        last_id = row["ID"]
    # Pick the first item, assuming the table is sorted in descending date order
    month_year_directory = table_data["Row"][0]["FileLeafRef"]
    # Construct URL - the filename seems to be the same each month
    return f"https://mainecare.maine.gov/PrvExclRpt/{month_year_directory}/PI0008-PM%20Monthly%20Exclusion%20Report.xlsx"


def crawl(context: Context) -> None:
    # First we find the link to the excel file
    excel_url = crawl_excel_url(context)
    path = context.fetch_resource("list.xlsx", excel_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)

    sheet_names = wb.sheetnames

    for item in h.parse_xlsx_sheet(context, wb.active, skiprows=26):
        crawl_item(item, context)

    sheet_names.remove(wb.active.title)

    for sheet_name in sheet_names:
        sheet = wb[sheet_name]
        if not (sheet.max_row == 1 and sheet.max_column == 1 and not sheet["A1"].value):
            context.log.warning(f"Sheet {sheet_name} is not empty")
