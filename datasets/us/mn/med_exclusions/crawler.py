from rigour.mime.types import XLSX
from openpyxl import load_workbook

from zavod import Context, helpers as h
from zavod.util import Element
from zavod.extract.zyte_api import fetch_html, fetch_resource


def crawl_item(row: dict[str, str | None], context: Context) -> None:
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


def unblock_validator(doc: Element) -> bool:
    return (
        len(h.xpath_elements(doc, ".//span[text()='MHCP Excluded Group Providers']/.."))
        > 0
    ) and (
        len(
            h.xpath_elements(
                doc, ".//span[text()='MHCP Excluded Individual Providers']/.."
            )
        )
        > 0
    )


def crawl_excel_urls(context: Context) -> tuple[str, str]:
    groups_xpath = ".//span[text()='MHCP Excluded Group Providers']/.."
    individuals_xpath = ".//span[text()='MHCP Excluded Individual Providers']/.."
    doc = fetch_html(context, context.data_url, groups_xpath, geolocation="US")
    groups_url = h.xpath_string(doc, groups_xpath + "/@href")
    individuals_url = h.xpath_string(doc, individuals_xpath + "/@href")
    assert groups_url is not None, "Could not find Group Providers Excel URL"
    assert individuals_url is not None, "Could not find Individual Providers Excel URL"
    return groups_url, individuals_url


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

        assert wb.active is not None
        for item in h.parse_xlsx_sheet(context, wb.active):
            crawl_item(item, context)
