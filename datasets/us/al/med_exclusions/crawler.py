from typing import Dict
import xlrd
from rigour.mime.types import XLS

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_html, fetch_resource


def unblock_validator(doc) -> bool:
    return len(doc.xpath("//li[a[contains(., 'Excel Version')]]")) > 0


def crawl_excel_url(context: Context):
    doc = fetch_html(context, context.data_url, unblock_validator=unblock_validator)
    doc.make_links_absolute(context.data_url)
    return doc.xpath("//a[contains(., 'Excel Version')]")[0].get("href")


def crawl(context: Context) -> None:
    # First we find the link to the excel file
    excel_url = crawl_excel_url(context)
    _, _, _, path = fetch_resource(
        context, "source.xls", excel_url, expected_media_type=XLS
    )
    context.export_resource(path, XLS, title=context.SOURCE_TITLE)
    wb = xlrd.open_workbook(path)

    for item in h.parse_xls_sheet(context, wb.active, skiprows=31):
        context.log.info(item)
        crawl_item(item, context)
