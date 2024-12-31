import shutil
from tempfile import NamedTemporaryFile, TemporaryFile, mkstemp
from typing import Dict, Generator
import zipfile
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

BRIGHT_USERNAME = os.environ.get("BRIGHTDATA_BROWSER_USERNAME")
BRIGHT_PASSWORD = os.environ.get("BRIGHTDATA_BROWSER_PASSWORD")
SBR_WS_CDP = f"https://{BRIGHT_USERNAME}:{BRIGHT_PASSWORD}@brd.superproxy.io:9222"


def fix_xlsx_empty_styles(path):
    """
    Deal with invalid empty Fill

    The error looks like this:

    ```
    TypeError: Fill() takes no arguments
    ... stack trace ...
    TypeError: expected <class 'openpyxl.styles.fills.Fill'>
    ```
    """
    with NamedTemporaryFile() as tmp:
        zin = zipfile.ZipFile(path, "r")
        zout = zipfile.ZipFile(tmp.name, "w")
        for item in zin.infolist():
            buffer = zin.read(item.filename)
            if item.filename == "xl/styles.xml":
                styles = buffer.decode("utf-8")
                styles = styles.replace("<x:fill />", "")
                buffer = styles.encode("utf-8")
            zout.writestr(item, buffer)
        zout.close()
        zin.close()
        os.replace(tmp.name, path)


async def download_file(path, page_url: str):
    async with async_playwright() as pw:
        browser = await pw.chromium.connect_over_cdp(SBR_WS_CDP)
        page = await browser.new_page()
        client = await page.context.new_cdp_session(page)
        await page.goto(page_url)
        await click_and_download(
            page,
            client,
            ".entitylist-download.btn",
            "https://ilhfspartner3.dynamics365portals.us/_services/download-as-excel/*",
            path,
        )


# {'provider_name': 'ZARLENGO PHILIP C',
#  'license': None,
#  'npi': None,
#  'provider_type': None,
#  'affiliation': 'AUGUSTINE MEDICAL INC',
#  'action_date': '2007-11-02',
#  'action_type': 'Barred',
#  'address': None,
#  'address_2': None,
#  'city': None,
#  'state': None,
#  'zip_code': None}
def crawl_item(context: Context, row: Dict[str, str]) -> None:

    name = row.pop("provider_name")
    npi = row.pop("npi")
    license = row.pop("license")
    city = row.pop("city")

    entity = context.make("LegalEntity")
    entity.id = context.make_id(name, npi, license, city)
    parts = name.split(" DBA ")
    name = parts[0]
    alias = parts[1:]
    entity.add("name", name)
    entity.add("alias", alias)
    entity.add("topics", "debarment")
    entity.add("country", "US")
    entity.add("sector", row.pop("provider_type"))

    sanction = h.make_sanction(context, entity)
    sanction.add("startDate", row.pop("action_date"))
    sanction.add("provisions", row.pop("action_type"))

    address = h.make_address(
        context,
        street=row.pop("address"),
        street2=row.pop("address_2"),
        city=city,
        state=row.pop("state"),
        postal_code=row.pop("zip_code"),
        country_code="US",
    )
    h.apply_address(context, entity, address)
    h.copy_address(entity, address)

    if affiliate := row.pop("affiliation"):
        affiliate_entity = context.make("LegalEntity")
        affiliate_entity.id = context.make_id(entity.id, affiliate)
        affiliate_entity.add("name", affiliate)

        rel = context.make("UnknownLink")
        rel.id = context.make_id(entity.id, "affiliate", affiliate)
        rel.add("subject", entity)
        rel.add("object", affiliate_entity)
        rel.add("role", "affiliate")

        context.emit(affiliate_entity)
        context.emit(rel)

    context.emit(entity, target=True)
    context.emit(sanction)

    context.audit_data(row)


def crawl(context: Context) -> None:
    path = dataset_data_path(context.dataset.name) / "source.xlsx"

    asyncio.run(download_file(path, context.data_url))
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    fix_xlsx_empty_styles(path)

    workbook: openpyxl.Workbook = openpyxl.load_workbook(path)
    for row in h.parse_xlsx_sheet(context, sheet=workbook["Active Sanctions"]):
        crawl_item(context, row)
