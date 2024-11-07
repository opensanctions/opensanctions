from typing import Dict
from rigour.mime.types import XLSX
from openpyxl import load_workbook
import re

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_html, fetch_resource

REGEX_DBA = re.compile(r"\bdba\b", re.IGNORECASE)
REGEX_AKA = re.compile(r"\(?a\.?k\.?a\b\.?|\)", re.IGNORECASE)


def crawl_individual(row: Dict[str, str], context: Context):

    entity = context.make("Person")
    entity.id = context.make_id(
        row.get("last_name"), row.get("first_name"), row.get("npi")
    )
    names = REGEX_AKA.split(row.pop("last_name"))
    last_name = names[0]
    alias = names[1:]

    names = REGEX_AKA.split(row.pop("first_name"))
    first_name = names[0]
    alias.extend(names[1:])

    h.apply_name(entity, first_name=first_name, last_name=last_name, alias=alias)
    entity.add("country", "us")
    entity.add("sector", row.pop("provider_type"))
    entity.add("topics", "debarment")
    if row.get("provider_id"):
        entity.add("description", "Provider ID: " + row.pop("provider_id"))
    h.apply_date(entity, "birthDate", row.pop("dob"))

    if row.get("npi"):
        npis = h.multi_split(row.pop("npi"), ["N/A", "n/a", "/", "\n", " "])

        entity.add("npiCode", npis)
    else:
        row.pop("npi")

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", row.pop("action_date"))
    sanction.add("status", row.pop("status"))

    context.emit(entity, target=True)
    context.emit(sanction)

    context.audit_data(row, ignore=["date_added"])


def crawl_organization(row: Dict[str, str], context: Context):

    entity = context.make("Company")
    entity.id = context.make_id(row.get("organization_name"))
    names = REGEX_DBA.split(row.pop("organization_name"))
    entity.add("name", names[0])
    entity.add("alias", names[1:])
    entity.add("country", "us")
    entity.add("sector", row.pop("provider_type"))
    entity.add("topics", "debarment")
    if row.get("provider_id"):
        entity.add("description", "Provider ID: " + row.pop("provider_id"))

    address = h.make_address(
        context,
        street=row.pop("address_1"),
        street2=row.pop("address_2"),
        state=row.pop("state"),
        city=row.pop("city"),
        postal_code=row.pop("zip_code"),
    )

    h.apply_address(context, entity, address)

    if row.get("npi"):
        npis = h.multi_split(row.pop("npi"), ["N/A", "n/a", "/", "\n", " "])

        entity.add("npiCode", npis)
    else:
        row.pop("npi")

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", row.pop("action_date"))
    sanction.add("status", row.pop("status"))

    context.emit(entity, target=True)
    context.emit(sanction)
    context.emit(address)

    context.audit_data(row, ignore=["date_added"])


def unblock_validator(doc) -> bool:
    return (
        len(
            doc.xpath(
                "//a[contains(text(), 'Ohio') and contains(text(), 'Medicaid') and contains(text(), 'Provider') and contains(text(), 'Exclusion') and contains(text(), 'Suspension') and contains(text(), 'List')]"
            )
        )
        > 0
    )


def crawl_excel_url(context: Context):
    doc = fetch_html(
        context, context.data_url, unblock_validator=unblock_validator, geolocation="US"
    )
    return doc.xpath(
        "//a[contains(text(), 'Ohio') and contains(text(), 'Medicaid') and contains(text(), 'Provider') and contains(text(), 'Exclusion') and contains(text(), 'Suspension') and contains(text(), 'List')]"
    )[0].get("href")


def crawl(context: Context) -> None:
    # First we find the link to the excel file
    excel_url = crawl_excel_url(context)
    _, _, _, path = fetch_resource(
        context, "source.xlsx", excel_url, expected_media_type=XLSX, geolocation="US"
    )
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)

    for item in h.parse_xlsx_sheet(context, wb["Individuals"]):
        crawl_individual(item, context)

    for item in h.parse_xlsx_sheet(context, wb["Organizations"]):
        crawl_organization(item, context)
