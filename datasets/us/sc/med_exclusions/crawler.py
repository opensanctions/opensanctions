from typing import Dict
from rigour.mime.types import XLSX
from openpyxl import load_workbook

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_html, fetch_resource
import re

REGEX_ALIAS = re.compile(r"\ba\.k\.a\.?|\baka\b|\bf/k/a\b|\bdba\b", re.IGNORECASE)


def crawl_item(row: Dict[str, str], context: Context):
    entity = context.make("LegalEntity")
    entity.id = context.make_id(row.get("individual_entity"), row.get("npi"))
    parts = REGEX_ALIAS.split(row.pop("individual_entity").strip())

    entity.add("name", parts[0])
    entity.add("alias", parts[1:])

    if row.get("npi") != "Not Found":
        for npi in row.pop("npi").split(","):
            entity.add("npiCode", npi)
    else:
        row.pop("npi")

    entity.add("topics", "debarment")
    entity.add("sector", row.pop("last_known_profession_provider_type"))
    entity.add("country", "us")

    address = h.make_address(
        context,
        city=row.pop("city"),
        state=row.pop("state"),
        postal_code=row.pop("zip"),
        country_code="US",
    )
    h.apply_address(context, entity, address)
    h.copy_address(entity, address)

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", row.pop("date_excluded"))

    context.emit(entity, target=True)
    context.emit(sanction)

    context.audit_data(row)


def unblock_validator(doc) -> bool:
    return (
        len(doc.xpath(".//a[contains(text(), 'South Carolina Excluded Providers')]"))
        > 0
    )


def crawl_excel_url(context: Context):
    doc = fetch_html(context, context.data_url, unblock_validator=unblock_validator)
    return doc.xpath(".//a[contains(text(), 'South Carolina Excluded Providers')]")[
        0
    ].get("href")


def crawl(context: Context) -> None:

    excel_url = crawl_excel_url(context)

    cached, group_path, mediatype, _charset = fetch_resource(
        context, "source.xlsx", excel_url, geolocation="US"
    )
    if not cached:
        assert mediatype == XLSX
    context.export_resource(group_path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(group_path, read_only=True)

    for item in h.parse_xlsx_sheet(context, wb.active, skiprows=2):
        crawl_item(item, context)
