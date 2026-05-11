from rigour.mime.types import XLSX
from openpyxl import load_workbook
import re

from zavod import Context, helpers as h
from zavod.extract import zyte_api

# Regular expression to match the comma before "Inc."
INC_PATTERN = r",\s*Inc\."


def crawl_item(row: dict[str, str], context: Context) -> None:
    if not row.get("name"):
        return

    names = row.pop("name").split("/")
    npi = row.pop("npi")
    kmap = row.pop("kmap_provider")
    dba = row.pop("d_b_a_business_name")
    if dba is not None:
        dba = re.sub(INC_PATTERN, " Inc.", dba)
    termination_date = row.pop("termination_date")
    comments = row.pop("comments")
    entity = context.make("LegalEntity")
    entity.id = context.make_id(names, row.get("npi"))

    entity.add("name", names)
    entity.add("alias", h.multi_split(dba, [" / ", ", "]))
    entity.add("country", "us")
    entity.add("npiCode", h.multi_split(npi, [";", "\n"]))
    entity.add("topics", "debarment")
    entity.add("sector", row.pop("provider_type"))
    if kmap != "N/A":
        entity.add("description", "KMAP Provider Number " + kmap)

    sanction = h.make_sanction(context, entity)
    sanction.add("startDate", termination_date)
    sanction.add("summary", comments)

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def crawl_excel_url(context: Context) -> str:
    file_xpath = "//*[text()='Termination List (XLSX)']"
    doc = zyte_api.fetch_html(
        context, context.data_url, unblock_validator=file_xpath, absolute_links=True
    )
    url = h.xpath_string(doc, file_xpath + "/@href")
    assert url is not None, "Could not find Excel file URL"
    return url


def crawl(context: Context) -> None:
    # First we find the link to the excel file
    excel_url = crawl_excel_url(context)
    _, _, _, path = zyte_api.fetch_resource(
        context, "source.xlsx", excel_url, expected_media_type=XLSX
    )
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)

    for item in h.parse_xlsx_sheet(context, wb.active, skiprows=1):
        crawl_item(item, context)
