from typing import Dict, Generator
from rigour.mime.types import XLSX
from openpyxl import load_workbook
from normality import slugify, stringify
from datetime import datetime
import openpyxl
from playwright.async_api import async_playwright
import os
import asyncio
from zavod.shed.playwright import click_and_download

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


async def download_file(path, page_url: str):
    async with async_playwright() as pw:
        print('Connecting to Scraping Browser...')
        browser = await pw.chromium.connect_over_cdp(SBR_WS_CDP)
        page = await browser.new_page()
        client = await page.context.new_cdp_session(page)
        print('Connected! Navigating to webpage')
        await page.goto(page_url)
        await click_and_download(page, client, ".entitylist-download.btn", "https://ilhfspartner3.dynamics365portals.us/_services/download-as-excel/*", path)


def crawl(context: Context) -> None:
    path = dataset_data_path(context.dataset.name) / "source.xlsx"
    asyncio.run(download_file(path, context.data_url))
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
