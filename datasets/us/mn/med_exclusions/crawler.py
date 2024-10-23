from typing import Dict
from rigour.mime.types import XLSX
from openpyxl import load_workbook

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_html, fetch_resource


def crawl_item(row: Dict[str, str], context: Context):

    address = h.make_address(
        context,
        city=row.pop("city"),
        # In one file it is called and in the other one they use st
        state=row.pop("state", row.pop("st", None)),
        postal_code=row.pop("zipcode", row.pop("zip", None)),
        country_code="US",
        street=row.pop("addressline1"),
        street2=row.pop("addressline2"),
    )

    if "sortname" in row:
        entity = context.make("Company")
        entity.id = context.make_id(
            row.get("sortname"), row.get("providertypedescription")
        )
        entity.add("name", row.pop("sortname"))
    else:
        entity = context.make("Person")
        entity.id = context.make_id(
            row.get("firstname"), row.get("middlename"), row.get("lastname")
        )
        h.apply_name(
            entity,
            first_name=row.pop("firstname"),
            last_name=row.pop("lastname"),
            middle_name=row.pop("middlename"),
        )
    entity.add("topics", "debarment")
    entity.add("sector", row.pop("providertypedescription"))
    entity.add("country", "us")
    h.apply_address(context, entity, address)

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", row.pop("effectivedateofexclusion"))

    context.emit(entity, target=True)
    context.emit(sanction)
    context.emit(address)

    context.audit_data(row)


def unblock_validator(doc) -> bool:
    return (
        len(doc.xpath(".//span[text()='MHCP Excluded Group Providers']/..")) > 0
    ) and (
        len(doc.xpath(".//span[text()='MHCP Excluded Individual Providers']/..")) > 0
    )


def crawl_excel_urls(context: Context):
    doc = fetch_html(
        context, context.data_url, geolocation="US", unblock_validator=unblock_validator
    )
    return doc.xpath(".//span[text()='MHCP Excluded Group Providers']/..")[0].get(
        "href"
    ), doc.xpath(".//span[text()='MHCP Excluded Individual Providers']/..")[0].get(
        "href"
    )


def crawl(context: Context) -> None:

    group_url, individuals_url = crawl_excel_urls(context)
    cached, group_path, mediatype, _charset = fetch_resource(
        context, "group.xlsx", group_url, geolocation="US"
    )
    if not cached:
        assert mediatype == XLSX
    context.export_resource(group_path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(group_path, read_only=True)

    for item in h.parse_xlsx_sheet(context, wb.active):
        crawl_item(item, context)

    cached, individuals_path, mediatype, _charset = fetch_resource(
        context, "individuals.xlsx", individuals_url, geolocation="US"
    )
    if not cached:
        assert mediatype == XLSX
    context.export_resource(individuals_path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(individuals_path, read_only=True)

    for item in h.parse_xlsx_sheet(context, wb.active):
        crawl_item(item, context)
