from typing import Dict
from rigour.mime.types import XLSX
from openpyxl import load_workbook

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_html

URL_XPATH = "//*[text()='List of Sanctioned Providers (XLSX)']/ancestor::a"


def crawl_item(row: Dict[str, str], context: Context):

    sanction_date_1 = row.pop("sanction_date1")
    sanction_date_1 = sanction_date_1.split() if sanction_date_1 else None
    sanction_date_2 = row.pop("sanction_date2")
    sanction_date_2 = sanction_date_2.split() if sanction_date_2 else None
    reason = row.pop("reason")
    license = row.pop("license")
    npi = row.pop("npi")
    provider_category = row.pop("provider_category")
    source = row.pop("sanction_source")
    city = row.pop("city")
    address = h.format_address(city=city, state="MI", country_code="US")

    entity = None
    person = None
    if entity_name := row.pop("entity_name"):
        entity = context.make("Company")
        entity.id = context.make_id(entity_name, npi)
        entity.add("name", entity_name)
        entity.add("npiCode", npi)
        entity.add("topics", "debarment")
        entity.add("sector", provider_category)
        entity.add("address", address)
        entity.add("country", "US")
        if license:
            entity.add("description", "License number: " + license)

        sanction = h.make_sanction(context, entity)
        if sanction_date_1:
            h.apply_dates(sanction, "startDate", sanction_date_1)
        if sanction_date_2:
            h.apply_dates(sanction, "startDate", sanction_date_2)
        sanction.add("publisher", source)
        sanction.add("reason", reason)

        context.emit(entity)
        context.emit(sanction)

    if last_name := row.pop("last_name"):
        person = context.make("Person")
        person.id = context.make_id(
            row.get("first_name"),
            row.get("middle_name"),
            last_name,
            npi,
            city,
        )
        h.apply_name(
            person,
            first_name=row.pop("first_name"),
            middle_name=row.pop("middle_name"),
            last_name=last_name,
        )
        person.add("topics", "debarment")
        person.add("npiCode", npi)
        person.add("sector", provider_category)
        person.add("address", address)
        person.add("country", "US")

        person_sanction = h.make_sanction(context, person)
        if sanction_date_1:
            h.apply_dates(person_sanction, "startDate", sanction_date_1)
        if sanction_date_2:
            h.apply_dates(person_sanction, "startDate", sanction_date_2)
        person_sanction.add("publisher", source)
        person_sanction.add("reason", reason)

        context.emit(person)
        context.emit(person_sanction)

    if last_name and entity_name:

        link = context.make("UnknownLink")
        link.id = context.make_id(person.id, entity.id)
        link.add("object", entity)
        link.add("subject", person)

        context.emit(link)

    context.audit_data(row)


def crawl_excel_url(context: Context):
    doc = fetch_html(
        context, context.data_url, URL_XPATH, html_source="httpResponseBody"
    )
    doc.make_links_absolute(context.data_url)
    links = doc.xpath(URL_XPATH)
    return links[0].get("href")


def crawl(context: Context) -> None:
    # First we find the link to the excel file
    excel_url = crawl_excel_url(context)
    path = context.fetch_resource("list.xlsx", excel_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)

    for item in h.parse_xlsx_sheet(context, wb.active, skiprows=1):
        crawl_item(item, context)
