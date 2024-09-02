from typing import Dict
from rigour.mime.types import XLSX
from openpyxl import load_workbook
import requests
from lxml import html
from io import BytesIO

from zavod import Context, helpers as h

def crawl_item(row: Dict[str, str], context: Context):

    name = row.pop("provider_name")

    if not name:
        return

    entity = context.make("LegalEntity")
    entity.id = context.make_id(name, row.get("national_provider_identifier_npi"))
    entity.add("name", name)
    entity.add("npiCode", row.pop("national_provider_identifier_npi"))
    entity.add("sector", row.pop("provider_type"))

    entity.add("topics", "debarment")

    sanction = h.make_sanction(context, entity)
    sanction.add("startDate", row.pop("suspension_exclusion_effective_date"))
    sanction.add("reason", row.pop("suspension_exclusion_reason"))

    context.emit(entity, target=True)
    context.emit(sanction)

    context.audit_data(row, ignore=["unique_id"])


def crawl_excel_url(context: Context):
    doc = html.fromstring(requests.get(context.data_url).text)
    doc.make_links_absolute(context.data_url)
    return doc.xpath("//*[contains(text(), 'XLSX')]/../@href")[0]


def crawl(context: Context) -> None:
    # First we find the link to the excel file
    excel_url = crawl_excel_url(context)
    excel_file = requests.get(excel_url).content

    wb = load_workbook(BytesIO(excel_file), read_only=True)

    for item in h.parse_xlsx_sheet(context, wb.active):
        crawl_item(item, context)