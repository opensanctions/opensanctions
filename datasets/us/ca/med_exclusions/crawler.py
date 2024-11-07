from typing import Dict
from rigour.mime.types import XLSX
from openpyxl import load_workbook

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_html, fetch_resource


def crawl_item(row: Dict[str, str], context: Context):
    addresses = row.pop("address_es")
    first_name = row.pop("first_name")
    middle_name = row.pop("middle_name")
    last_name = row.pop("last_name")
    aliases = row.pop("a_k_a_also_known_asd_b_a_doing_business_as")

    if first_name != "N/A":
        entity = context.make("Person")
        entity.id = context.make_id(first_name, middle_name, last_name, addresses)

        h.apply_name(
            entity,
            first_name=first_name,
            middle_name=middle_name if middle_name != "N/A" else None,
            last_name=last_name,
        )
        if aliases != "N/A":
            entity.add("alias", [a.strip() for a in aliases.split(";")])

    else:
        entity = context.make("Company")
        entity.id = context.make_id(last_name, addresses)
        entity.add("name", last_name)

        if aliases != "N/A":
            for alias in aliases.split(";"):
                related_entity = context.make("LegalEntity")
                related_entity.id = context.make_id(alias, entity.id)
                related_entity.add("name", alias.strip())
                rel = context.make("UnknownLink")
                rel.id = context.make_id(entity.id, related_entity.id)
                rel.add("subject", entity)
                rel.add("object", related_entity)
                context.emit(related_entity)
                context.emit(rel)

    entity.add("country", "us")
    entity.add("topics", "debarment")
    entity.add("sector", row.pop("provider_type"))
    entity.add("address", h.multi_split(addresses, [", &", ";"]))

    license_number = row.pop("license_number")
    if license_number and license_number != "N/A":
        entity.add("description", "License number(s): " + license_number)

    provider_number = row.pop("provider_number")
    if provider_number and provider_number != "N/A":
        entity.add("description", "Provider number(s): " + provider_number)

    sanction = h.make_sanction(context, entity)
    start_date = row.pop("date_of_suspension")
    if start_date != "N/A":
        h.apply_date(sanction, "startDate", start_date)
    sanction.add("duration", row.pop("active_period"))

    context.emit(entity, target=True)
    context.emit(sanction)

    context.audit_data(row)


def unblock_validator(doc) -> bool:
    xpath = ".//a[contains(text(), 'Medi-Cal Suspended and Ineligible Provider List')]"
    return len(doc.xpath(xpath)) > 0


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
    _, _, _, path = fetch_resource(
        context, "source.xlsx", excel_url, expected_media_type=XLSX, geolocation="US"
    )
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)

    for item in h.parse_xlsx_sheet(context, wb.active):
        crawl_item(item, context)
