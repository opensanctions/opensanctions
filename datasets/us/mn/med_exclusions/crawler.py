from typing import Dict
from rigour.mime.types import XLSX
from openpyxl import load_workbook

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_html, fetch_resource


def crawl_item(row: Dict[str, str], context: Context):
    is_company = "practice_name" in row

    address = h.make_address(
        context,
        city=row.pop("city"),
        state=row.pop("state"),
        postal_code=row.pop("zip_code"),
        country_code="US",
        street=row.pop("address_line1"),
        street2=row.pop("address_line2"),
    )

    if is_company:
        entity = context.make("Company")
        entity.id = context.make_id(
            row.get("practice_name"),
            row.get("zip_code"),
        )
        entity.add("name", row.pop("practice_name"))
    else:
        entity = context.make("Person")
        entity.id = context.make_id(
            row.get("first_name"),
            row.get("middle_name"),
            row.get("last_name"),
            row.get("zip_code"),
        )
        h.apply_name(
            entity,
            first_name=row.pop("first_name"),
            last_name=row.pop("last_name"),
            middle_name=row.pop("middle_name"),
        )
    entity.add("topics", "debarment")
    entity.add("sector", row.pop("provider_type_description"))
    entity.add("country", "us")
    h.apply_address(context, entity, address)

    start_date = row.pop("effective_date_of_exclusion")
    sanction = h.make_sanction(context, entity, key=start_date)
    sanction.add("authority", row.pop("exclusion_status"))
    h.apply_date(sanction, "startDate", start_date)

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def unblock_validator(doc) -> bool:
    return (
        len(doc.xpath(".//span[text()='MHCP Excluded Group Providers']/..")) > 0
    ) and (
        len(doc.xpath(".//span[text()='MHCP Excluded Individual Providers']/..")) > 0
    )


def crawl_excel_urls(context: Context):
    groups_xpath = ".//span[text()='MHCP Excluded Group Providers']/.."
    individuals_xpath = ".//span[text()='MHCP Excluded Individual Providers']/.."
    doc = fetch_html(context, context.data_url, groups_xpath, geolocation="US")
    return (
        doc.xpath(groups_xpath)[0].get("href"),
        doc.xpath(individuals_xpath)[0].get("href"),
    )


def crawl(context: Context) -> None:
    group_url, individuals_url = crawl_excel_urls(context)
    urls = [(group_url, "group"), (individuals_url, "individuals")]

    for url, title_suffix in urls:
        resource_name = f"{title_suffix}.xlsx"
        _, _, _, file_path = fetch_resource(
            context,
            resource_name,
            url,
            geolocation="US",
            expected_media_type=XLSX,
        )

        context.export_resource(file_path, XLSX, title=context.SOURCE_TITLE)
        wb = load_workbook(file_path, read_only=True)

        for item in h.parse_xlsx_sheet(context, wb.active):
            crawl_item(item, context)
