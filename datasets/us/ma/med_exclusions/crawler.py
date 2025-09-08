from typing import Dict
from openpyxl import load_workbook

from zavod import Context, helpers as h
from zavod.shed import zyte_api


HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-GB,en;q=0.9",
    "Priority": "u=0, i",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
}


def crawl_item(row: Dict[str, str], context: Context):

    name = row.pop("provider_name")

    if not name:
        return

    entity = context.make("LegalEntity")
    entity.id = context.make_id(name, row.get("national_provider_identifier_npi"))
    entity.add("name", name)
    entity.add("npiCode", row.pop("national_provider_identifier_npi"))
    entity.add("sector", row.pop("provider_type"))
    entity.add("country", "us")
    entity.add("idNumber", row.pop("unique_id"))

    entity.add("topics", "debarment")

    sanction = h.make_sanction(context, entity)
    sanction.add("startDate", row.pop("suspension_exclusion_effective_date"))
    sanction.add("reason", row.pop("suspension_exclusion_reason"))

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def crawl_excel_url(context: Context):
    xlsx_xpath = "//*[contains(text(), 'XLSX')]/../@href"
    doc = zyte_api.fetch_html(context, context.data_url, unblock_validator=xlsx_xpath)
    doc.make_links_absolute(context.data_url)
    return doc.xpath(xlsx_xpath)[0]


def crawl(context: Context) -> None:
    # First we find the link to the excel file
    excel_url = crawl_excel_url(context)

    _, _, _, path = zyte_api.fetch_resource(context, "source.xlsx", excel_url)
    context.export_resource(path, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)

    for item in h.parse_xlsx_sheet(context, wb["Exclusions List"]):
        crawl_item(item, context)
