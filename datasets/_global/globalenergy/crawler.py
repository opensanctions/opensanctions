from openpyxl import load_workbook
from typing import Dict
from pantomime.types import XLSX

from zavod import Context
from zavod import helpers as h


def crawl_row(context: Context, row: Dict[str, str]):
    id = row.pop("Entity ID")
    name = row.pop("Entity Name")
    # abbreviation = row.pop("Abbreviation", "")
    # public_listing = row.pop("Public Listing", "")
    # website = row.pop("Website", "")
    reg_country = row.pop("Registration Country", "")
    # subdivision = row.pop("Subdivision", "")
    # headquarters = row.pop("Headquarters Country", "")
    # headquarters_subdivision = row.pop("Headquarters Subdivision", "")
    # parent = row.pop("Parents", "")
    # parent_id = row.pop("Parent Entity IDs", "")
    legal_id = row.pop("Legal Entity Identifier", "")
    # refinitiv_id = row.pop("Refinitiv PermID", "")
    # sec_index = row.pop("SEC Central Index Key", "")
    # eia_860 = row.pop("US EIA 860", "")

    entity = context.make("LegalEntity")
    entity.id = context.make_id(name, id, reg_country, legal_id)
    entity.add("name", name)
    entity.add("originalName", row.pop("Name Local", ""))
    entity.add("alias", row.pop("Name Other", ""))
    entity.add("idNumber", legal_id)
    entity.add("description", row.pop("Entity Type", ""))

    context.emit(entity)
    context.audit_data(row, ignore=["sequence_no", "decision_date"])


def crawl(context: Context):
    path = context.fetch_resource("source.xlsx", context.data_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)
    wb = load_workbook(path, read_only=True)
    for sheet in wb.worksheets:
        for row in h.parse_xlsx_sheet(context, sheet, header_lookup="columns"):
            crawl_row(context, row)
