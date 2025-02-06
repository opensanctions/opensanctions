from typing import Dict
from rigour.mime.types import CSV

from zavod import Context, helpers as h
import csv


def crawl_item(row: Dict[str, str], context: Context):
    start_date = row.pop("BeginDate")
    end_date = row.pop("EndDate")
    status = row.pop("Status")
    listing_date = row.pop("ListDate")
    license_number = row.pop("LicenseNumber")
    alias = row.pop("NAM_PROVR_ALT")
    npi = row.pop("IDN_NPI")
    title = row.pop("NAM_TITLE_PROVR")

    if row.get("CAO").startswith("Out of State"):
        state = row.pop("CAO").replace("Out of State ", "")
        city = ""
    else:
        city = row.pop("CAO")
        state = "PA"
    address = h.make_address(context, city=city, state=state, country_code="us")

    if last_name := row.pop("NAM_LAST_PROVR"):
        person = context.make("Person")
        person.id = context.make_id(
            last_name, row.get("NAM_FIRST_PROVR"), row.get("NAM_MIDDLE_PROVR")
        )
        h.apply_name(
            person,
            first_name=row.pop("NAM_FIRST_PROVR"),
            middle_name=row.pop("NAM_MIDDLE_PROVR"),
            last_name=last_name,
            suffix=row.pop("NAM_SUFFIX_PROVR"),
        )
        h.apply_address(context, person, address)

        if row.get("NAM_PROVR_ALT"):
            person.add("alias", row.pop("NAM_PROVR_ALT"))

        person.add("npiCode", npi)
        person.add("title", title)
        person.add("topics", "debarment")
        if license_number:
            person.add("idNumber", license_number)
        person.add("country", "us")

        sanction = h.make_sanction(context, person)
        sanction.add("status", status)
        h.apply_date(sanction, "startDate", start_date)

        if row.get("EndDate"):
            h.apply_date(sanction, "endDate", end_date)

        if row.get("ListDate"):
            h.apply_date(sanction, "listingDate", listing_date)

        context.emit(person)
        context.emit(sanction)

    if business_name := row.pop("NAM_BUSNS_MP"):
        company = context.make("Company")
        company.id = context.make_id(business_name)
        company.add("name", business_name)
        company.add("topics", "debarment")
        company.add("taxNumber", row.pop("NBR_FEIN"))
        company.add("npiCode", npi)
        company.add("country", "us")

        h.apply_address(context, company, address)

        if alias:
            company.add("alias", alias)

        if license_number:
            company.add("idNumber", license_number)

        company_sanction = h.make_sanction(context, company)
        company_sanction.add("status", status)
        h.apply_date(company_sanction, "startDate", start_date)

        if row.get("EndDate"):
            h.apply_date(company_sanction, "endDate", end_date)

        if row.get("ListDate"):
            h.apply_date(company_sanction, "listingDate", listing_date)

        context.emit(company)
        context.emit(company_sanction)

    if last_name and business_name:
        link = context.make("UnknownLink")
        link.id = context.make_id(company.id, person.id)
        link.add("object", company)
        link.add("subject", person)
        context.emit(link)

    context.audit_data(row, ignore=["IND_CHGD", "DTE_CHANGE_LAST", "ProviderName"])


def crawl(context: Context) -> None:
    path = context.fetch_resource("list.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)

    with open(path, "r") as fh:
        for item in csv.DictReader(fh):
            crawl_item(item, context)
