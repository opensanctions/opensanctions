from typing import Dict
from rigour.mime.types import XLSX
from openpyxl import load_workbook

from zavod import Context
from zavod.extract.zyte_api import fetch_html, fetch_resource
from zavod import helpers as h


def crawl_item(row: Dict[str, str], context: Context):
    entity = context.make("LegalEntity")
    entity.id = context.make_id(row.get("provider_name"), row.get("npi"))
    entity.add("name", row.pop("provider_name"))
    if row.get("npi") != "N/A":
        entity.add("npiCode", row.pop("npi"))
    else:
        row.pop("npi")
    entity.add("topics", "debarment")
    entity.add("sector", row.pop("prov_type_specialty"))
    if row.get("license") and row.get("license") != "N/A":
        entity.add("description", "License number: " + row.pop("license"))
    else:
        row.pop("license")
    entity.add("country", "us")

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", row.pop("term_date"))
    h.apply_date(sanction, "date", row.pop("letter_date"))
    sanction.add("reason", row.pop("termination_reason"))

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def crawl_excel_url(context: Context):
    """
    Navigate from the main page to the exclusions page and extract
    the direct download URL for the exclusions Excel file.

    Flow:
    1. Main page → exclusions page
    2. Exclusions page → Excel download link
    """
    exclusions_page_xpath = "//a[starts-with(@href, 'https://mmac.mo.gov/')][contains(@href, 'sanction-list') or contains(@href, 'Sanction-List')]/@href"
    main_page = fetch_html(
        context,
        context.data_url,
        exclusions_page_xpath,
    )
    exclusions_page_url = h.xpath_string(main_page, exclusions_page_xpath)
    assert exclusions_page_url, exclusions_page_url

    # Fetch the page that contains the Excel download link
    excel_download_xpath = "//div[@class='entry']//a[contains(@href, 'Sanction-List') and contains(@href, '.xlsx')]/@href"
    exclusions_page = fetch_html(
        context,
        exclusions_page_url,
        excel_download_xpath,
    )
    excel_url = h.xpath_string(exclusions_page, excel_download_xpath)
    return excel_url


def crawl(context: Context) -> None:
    # First we find the link to the excel file
    excel_url = crawl_excel_url(context)
    _, _, _, path = fetch_resource(
        context, "source.xlsx", excel_url, expected_media_type=XLSX, geolocation="US"
    )
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)
    wb = load_workbook(path, read_only=True)

    if len(wb.sheetnames) > 1:
        context.log.warning("Additional sheet found")

    for item in h.parse_xlsx_sheet(context, wb.active, skiprows=1):
        crawl_item(item, context)
