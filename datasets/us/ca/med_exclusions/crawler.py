from typing import Dict
from rigour.mime.types import XLSX
from openpyxl import load_workbook
from datetime import datetime

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_html, fetch_resource


def crawl_item(row: Dict[str, str], context: Context):

    if row.get("first_name") != "N/A":
        entity = context.make("Person")
        entity.id = context.make_id(
            row.get("first_name"), row.get("middle_name"), row.get("last_name")
        )
        
        h.apply_name(
            entity,
            first_name=row.pop("first_name"),
            last_name=row.pop("last_name"),
        )

        if row.get("middle_name") != "N/A":
            h.apply_name(entity, middle_name=row.pop("middle_name"))
        else:
            row.pop("middle_name")

    else:
        row.pop("first_name")
        row.pop("middle_name")
        entity = context.make("Company")
        entity.id = context.make_id(row.get("last_name"))
        entity.add("name", row.pop("last_name"))

    entity.add("country", "us")
    entity.add("topics", "debarment")
    entity.add("sector", row.pop("provider_type"))
    addresses = [
        h.make_address(context, full=add_text)
        for add_text in row.pop("address_es").split(";")
        # there are some cases where when we split it returns an empty string
        if add_text
    ]
    for address in addresses:
        entity.add("address", address)
        h.apply_address(context, entity, address)
        context.emit(address)

    if row.get("a_k_a_also_known_asd_b_a_doing_business_as") != "N/A":
        entity.add(
            "alias", row.pop("a_k_a_also_known_asd_b_a_doing_business_as").split(";")
        )
    else:
        row.pop("a_k_a_also_known_asd_b_a_doing_business_as")

    if row.get("license_number") and row.get("license_number") != "N/A":
        entity.add("description", "License number(s): " + row.pop("license_number"))
    else:
        row.pop("license_number")

    if row.get("provider_number") and row.get("provider_number") != "N/A":
        entity.add("description", "Provider number(s): " + row.pop("provider_number"))
    else:
        row.pop("provider_number")

    sanction = h.make_sanction(context, entity)
    if row.get("date_of_suspension") != "N/A":
        h.apply_date(sanction, "startDate", row.pop("date_of_suspension"))
    else:
        row.pop("date_of_suspension")

    context.emit(entity, target=True)
    context.emit(sanction)

    # active_period is either Indefinetly or Deceased
    context.audit_data(row, ignore=["active_period"])


def unblock_validator(doc) -> bool:
    return (
        len(
            doc.xpath(
                ".//a[contains(text(), 'Medi-Cal Suspended and Ineligible Provider List')]"
            )
        )
        > 0
    )


def crawl_excel_url(context: Context):
    doc = fetch_html(
        context, context.data_url, geolocation="US", unblock_validator=unblock_validator
    )
    doc.make_links_absolute(context.data_url)
    return doc.xpath(
        ".//a[contains(text(), 'Medi-Cal Suspended and Ineligible Provider List')]"
    )[0].get("href")


def crawl(context: Context) -> None:
    # First we find the link to the excel file
    excel_url = crawl_excel_url(context)
    cached, path, mediatype, _charset = fetch_resource(
        context, "source.xlsx", excel_url, geolocation="US"
    )
    if not cached:
        assert mediatype == XLSX
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)

    for item in h.parse_xlsx_sheet(context, wb.active):
        crawl_item(item, context)
