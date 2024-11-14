from rigour.mime.types import XLSX
from openpyxl import load_workbook

from zavod import Context
from zavod.shed.zyte_api import fetch_html, fetch_resource

EXCEL_XPATH = "//a[starts-with(@href, 'https://mmac.mo.gov/')][contains(@href, 'Sanction')][contains(@href, 'xlsx')]"


def unblock_validator(doc) -> bool:
    return len(doc.xpath(EXCEL_XPATH)) > 0


def crawl_excel_url(context: Context):
    doc = fetch_html(context, context.data_url, unblock_validator=unblock_validator)
    links = doc.xpath(EXCEL_XPATH)[0].get("href")
    assert len(links) == 1, links
    return links[0]


def crawl(context: Context) -> None:
    # First we find the link to the excel file
    excel_url = crawl_excel_url(context)
    _, _, _, path = fetch_resource(
        context, "source.xlsx", excel_url, expected_media_type=XLSX, geolocation="US"
    )
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)
    wb = load_workbook(path, read_only=True)
    print(wb.sheetnames)
