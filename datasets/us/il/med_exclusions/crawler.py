from typing import Dict, Generator
from rigour.mime.types import XLSX
from openpyxl import load_workbook
from normality import slugify, stringify
from datetime import datetime
import openpyxl
from playwright.sync_api import sync_playwright
import os

from zavod import Context, helpers as h
from zavod.archive import dataset_data_path

AUTH = os.environ.get("BRIGHTDATA_BROWSER_CREDENTIALS")
SBR_WS_CDP = f'https://{AUTH}@brd.superproxy.io:9222'


def crawl_item(row: Dict[str, str], context: Context, is_excluded: bool):

    name = row.pop("provider-name")

    if not name:
        return

    entity = context.make("LegalEntity")
    entity.id = context.make_id(name)
    entity.add("name", name)
    entity.add("alias", row.pop("doing-business-as-name"))
    if row.get("npi") != "N/A":
        entity.add("notes", "NPI: " + row.pop("npi"))
    else:
        row.pop("npi")
    if is_excluded:
        entity.add("topics", "debarment")

    sanction = h.make_sanction(context, entity)
    sanction.add("startDate", row.pop("termination-effective-date"))
    sanction.add("reason", row.pop("termination-authority"))

    if row.get("reinstatement-effective-date"):
        sanction.add("endDate", row.pop("reinstatement-effective-date"))

    context.emit(entity, target=is_excluded)
    context.emit(sanction)

    context.audit_data(row)


def crawl_excel_url(context: Context):
    doc = context.fetch_html(context.data_url)
    return doc.find(".//*[@about='/provider-termination']").find(".//a").get("href")


def crawl(context: Context) -> None:
    path = dataset_data_path(context.dataset.name) / "source.xlsx"

    with sync_playwright() as pw:
        print('Connecting to Scraping Browser...')
        browser = pw.chromium.launch() #.connect_over_cdp(SBR_WS_CDP)
        page = browser.new_page()
        print('Connected! Navigating to webpage')
        page.goto(context.data_url)

        with page.expect_download() as download_info:
            # Perform the action that initiates download
            page.click(".entitylist-download.btn")
        download = download_info.value

        # Wait for the download process to complete and save the downloaded file somewhere
        download.save_as(path)


    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    workbook: openpyxl.Workbook = openpyxl.load_workbook(path, read_only=True)
    for row in h.parse_xlsx_sheet(context, sheet=workbook["Active Sanctions"]):
        print(row)
    ## Currently terminated providers
    #for item in parse_sheet(context, wb["Termination List"]):
    #    crawl_item(item, context, True)
#
    ## Providers that where terminated but are now reinstated
    #for item in parse_sheet(context, wb["Reinstated Providers"]):
    #    crawl_item(item, context, False)
