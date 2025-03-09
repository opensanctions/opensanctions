from typing import Dict
from openpyxl import load_workbook

from zavod import Context, helpers as h


def crawl_item(row: Dict[str, str], context: Context):
    first_name = row.pop("provider_individual_first_name")
    business_name = row.pop("business_name")
    npi = row.pop("national_provider_identifier_npi")
    sector = row.pop("provider_individual_type")
    reason = row.pop("exclusion_sanction_reason")
    # There is one line with multiple dates, we are only going to consider the first one
    startDate = row.pop("exclusion_sanction_effective_date").split(" ")[0]

    if first_name:
        person = context.make("Person")
        person.id = context.make_id(first_name, npi)
        h.apply_name(
            person,
            first_name=first_name,
            last_name=row.pop("provider_individual_last_name"),
        )
        person.add("alias", business_name)
        person.add("country", "us")
        person.add("npiCode", npi if npi and npi.lower() != "n/a" else None)
        person.add("sector", sector)
        person.add("topics", "debarment")
        person_sanction = h.make_sanction(context, person)
        person_sanction.add("reason", reason)
        h.apply_date(person_sanction, "startDate", startDate)

        context.emit(person)
        context.emit(person_sanction)
    if business_name and not first_name:
        company = context.make("Company")
        company.id = context.make_id(business_name, npi)
        company.add("name", business_name)
        company.add("country", "us")
        company.add("npiCode", npi if npi and npi.lower() != "n/a" else None)
        company.add("sector", sector)
        company.add("topics", "debarment")
        company_sanction = h.make_sanction(context, company)
        company_sanction.add("reason", reason)
        h.apply_date(company_sanction, "startDate", startDate)

        context.emit(company)
        context.emit(company_sanction)

    context.audit_data(row)


def crawl_excel_url(context: Context):
    doc = context.fetch_html(context.data_url)
    doc.make_links_absolute(context.data_url)
    return doc.xpath(
        "//*[contains(text(), 'Medicaid Provider Exclusion and Sanction List')]"
    )[0].get("href")


def crawl(context: Context) -> None:
    # First we find the link to the excel file
    excel_url = crawl_excel_url(context)

    path = context.fetch_resource("source.xlsx", excel_url)
    context.export_resource(path, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)

    for item in h.parse_xlsx_sheet(context, wb.active, skiprows=2):
        crawl_item(item, context)
