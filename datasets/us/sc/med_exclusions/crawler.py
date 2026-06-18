from rigour.mime.types import XLSX
from openpyxl import load_workbook

from zavod import Context, helpers as h
from zavod.extract import zyte_api
import re

REGEX_ALIAS = re.compile(r"\ba\.k\.a\.?|\baka\b|\bf/k/a\b|\bdba\b", re.IGNORECASE)


def crawl_item(row: dict[str, str | None], context: Context) -> None:
    entity = context.make("LegalEntity")
    npi = row.pop("npi")
    entity.id = context.make_id(row.get("individual_entity"), npi)
    parts = REGEX_ALIAS.split((row.pop("individual_entity") or "").strip())

    entity.add("name", parts[0])
    entity.add("alias", parts[1:])

    if npi != "Not Found":
        entity.add("npiCode", h.multi_split(npi, [" ", ",", "/"]))

    entity.add("topics", "debarment")
    entity.add("sector", row.pop("last_known_profession_provider_type"))
    entity.add("country", "us")

    address = h.format_address(
        city=row.pop("city"),
        state=row.pop("state"),
        postal_code=row.pop("zip"),
        country_code="US",
    )
    entity.add("address", address)

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", row.pop("action_date"))
    sanction.add("status", row.pop("excluded_terminated"))

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def crawl_excel_url(context: Context) -> str:
    file_xpath = ".//a[contains(text(), 'South Carolina Medicaid Excluded/Terminated Providers')]"
    doc = zyte_api.fetch_html(context, context.data_url, unblock_validator=file_xpath)
    url = h.xpath_string(doc, file_xpath + "/@href")
    assert url is not None, "Could not find Excel file URL"
    return url


def crawl(context: Context) -> None:
    excel_url = crawl_excel_url(context)

    _, _, _, path = zyte_api.fetch_resource(
        context, "source.xlsx", excel_url, geolocation="US", expected_media_type=XLSX
    )
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)
    assert wb.active is not None
    for item in h.parse_xlsx_sheet(context, wb.active, skiprows=2):
        crawl_item(item, context)
