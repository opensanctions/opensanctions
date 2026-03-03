from datetime import datetime
from typing import Dict

from openpyxl import load_workbook
from rigour.mime.types import XLSX
from zavod.extract import zyte_api

from zavod import Context
from zavod import helpers as h


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


def crawl(context: Context) -> None:
    table_xpath = "//table[@summary='Monthly Provider Exclusion Report']"
    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        table_xpath,
        geolocation="us",
        cache_days=3,
        absolute_links=True,
    )
    table = h.xpath_element(doc, table_xpath)
    rows = list(h.parse_html_table(table, index_empty_headers=True))
    str_rows = [h.cells_to_str(row) for row in rows]
    dates = [datetime.strptime(row["name"], "%B %Y") for row in str_rows]
    assert dates == sorted(dates, reverse=True), "Rows not in descending order"
    xlsx_page = h.xpath_string(rows[0]["name"], ".//a/@href")

    xlsx_doc = zyte_api.fetch_html(
        context,
        xlsx_page,
        "//table",
        geolocation="us",
        cache_days=3,
        absolute_links=True,
        javascript=True,
    )
    xlsx_url = h.xpath_string(
        xlsx_doc, ".//a[@class='ms-listlink' and contains(@href, '.xlsx')]/@href"
    )
    _, _, _, path = zyte_api.fetch_resource(
        context, filename="list.xlsx", url=xlsx_url, geolocation="us"
    )
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
