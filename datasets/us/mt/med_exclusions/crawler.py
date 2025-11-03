import re
from typing import Dict

from openpyxl import load_workbook
from rigour.mime.types import XLSX

from zavod import Context
from zavod import helpers as h

REGEX_AKA = re.compile(r"^a\.k\.a\.? ", re.IGNORECASE)


def crawl_item(row: Dict[str, str], context: Context):
    if ", " not in row.get("terminated_excluded_provider_s"):
        entity = context.make("Company")
        entity.id = context.make_id(row.get("terminated_excluded_provider_s"))
        entity.add("name", row.pop("terminated_excluded_provider_s"))
    else:
        entity = context.make("Person")
        entity.id = context.make_id(row.get("terminated_excluded_provider_s"))
        last_name, first_name = row.pop("terminated_excluded_provider_s").split(", ")
        h.apply_name(entity, first_name=first_name, last_name=last_name)

    entity.add("topics", "debarment")
    entity.add("sector", row.pop("healthcare_profession"))
    entity.add("country", "us")
    entity.add("npiCode", row.pop("npi"))

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", row.pop("effective_date"))

    context.audit_data(row, ignore=["column_4"])
    return entity, sanction


def crawl_excel_url(context: Context):
    doc = context.fetch_html(context.data_url, absolute_links=True)
    return doc.xpath(
        ".//a[text()='Download Excluded or Terminated Provider list in Excel']"
    )[0].get("href")


def crawl(context: Context) -> None:
    excel_url = crawl_excel_url(context)

    path = context.fetch_resource("list.xlsx", excel_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)

    entity = None
    sanction = None
    current_alias = []
    for item in h.parse_xlsx_sheet(context, wb.active):
        name = item.get("terminated_excluded_provider_s")
        if REGEX_AKA.match(name):
            current_alias.append(REGEX_AKA.sub("", name))
        else:
            # Move on to the next entity
            if entity:
                entity.add("name", current_alias)
                context.emit(entity)
                context.emit(sanction)
            current_alias = []
            entity, sanction = crawl_item(item, context)
    # Emit the last entity
    context.emit(entity)
    context.emit(sanction)
