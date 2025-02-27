from rigour.mime.types import PDF
from typing import Dict
import re

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_html, fetch_resource


AKA_PATTERN = r"\ba\.?k\.?a[\. -]*"
PAGE_SETTINGS = {"join_y_tolerance": 100}


def crawl_item(row: Dict[str, str], context: Context):
    address = h.make_address(
        context, city=row.pop("city"), state=row.pop("state"), country_code="US"
    )
    sector = row.pop("provider_type")
    provider_number = row.pop("provider_number")
    exclusion_date = row.pop("exclusion_date")
    last_name = row.pop("last_name")
    first_name = row.pop("first_name")
    business_name = row.pop("business_name")

    if not last_name and (first_name and business_name):
        context.log.info("Fixing shifted names", [last_name, first_name, business_name])
        last_name = first_name
        first_name = business_name
        business_name = None

    if last_name:
        person = context.make("Person")
        person.id = context.make_id(row.get("first_name"), last_name, provider_number)
        h.apply_name(person, first_name=first_name, last_name=last_name)
        h.apply_address(context, person, address)
        person.add("sector", sector)
        person.add("description", "Provider Number: " + provider_number)
        person.add("topics", "debarment")
        person.add("country", "us")

        sanction = h.make_sanction(context, person)
        h.apply_date(sanction, "startDate", exclusion_date)

    is_business = False

    if business_name:
        if re.match(AKA_PATTERN, business_name, flags=re.IGNORECASE):
            cleaned_name = re.sub(AKA_PATTERN, "", business_name, flags=re.IGNORECASE)
            person.add("alias", cleaned_name)
        else:
            is_business = True
            company = context.make("Company")
            company.id = context.make_id(business_name, provider_number)
            company.add("name", business_name)
            h.apply_address(context, company, address)
            company.add("sector", sector)
            company.add("description", "Provider Number: " + provider_number)
            company.add("topics", "debarment")
            company.add("country", "us")

            company_sanction = h.make_sanction(context, company)
            h.apply_date(company_sanction, "startDate", exclusion_date)

            context.emit(company)
            context.emit(company_sanction)

    # we have to emit the person latter because of the possibility of alias
    if last_name:
        context.emit(person)
        context.emit(sanction)

    if last_name and is_business:
        link = context.make("UnknownLink")
        link.id = context.make_id(person.id, company.id)
        link.add("object", company)
        link.add("subject", person)

        context.emit(link)

    context.audit_data(row)


def crawl_excel_url(context: Context):
    file_xpath = ".//a[text()='Wyoming Provider Exclusion List ']"
    doc = fetch_html(context, context.data_url, file_xpath, geolocation="us")
    return doc.xpath(file_xpath)[0].get("href")


def crawl(context: Context) -> None:
    url = crawl_excel_url(context)
    _, _, _, path = fetch_resource(
        context, "source.pdf", url, geolocation="us", expected_media_type=PDF
    )

    context.export_resource(path, PDF, title=context.SOURCE_TITLE)
    for item in h.parse_pdf_table(
        context,
        path,
        skiprows=1,
        page_settings=lambda page: (page, PAGE_SETTINGS),
    ):
        crawl_item(item, context)
