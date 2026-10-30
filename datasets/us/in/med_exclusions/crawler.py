from python_calamine import CalamineWorkbook
from rigour.mime.types import XLSX

from zavod import Context, helpers as h


def crawl_item(context: Context, row: dict[str, str | None]) -> None:
    name = row.pop("provider_name")

    if not name:
        return

    if "for purposes of" in name.lower():
        return

    entity = context.make("LegalEntity")
    entity.id = context.make_id(name, row.get("national_provider_identification_npi"))
    entity.add("name", name)
    entity.add("npiCode", row.pop("national_provider_identification_npi"))
    entity.add("address", (row.pop("service_location_address") or "").split("\n"))
    entity.add("country", "us")

    sanction = h.make_sanction(context, entity)
    sanction.add("startDate", row.pop("termination_effective_date"))

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def crawl_excel_url(context: Context) -> str:
    doc = context.fetch_html(context.data_url, absolute_links=True)
    excel_url = h.xpath_elements(
        doc, "//*[text()='Terminated providers']", expect_exactly=1
    )[0].get("href")
    assert excel_url is not None
    return excel_url


def crawl(context: Context) -> None:
    # First we find the link to the excel file
    excel_url = crawl_excel_url(context)
    path = context.fetch_resource("list.xlsx", excel_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = CalamineWorkbook.from_path(path)
    sheet = wb.get_sheet_by_name("Sheet1")

    for row in h.parse_xlsx_sheet(context, sheet, skiprows=1):
        crawl_item(context, row)
