from typing import Dict
from openpyxl import load_workbook

from zavod import Context, helpers as h


def crawl_item(row: Dict[str, str], context: Context):

    npiCode = row.pop("national_provider_identifier_npi")
    sector = row.pop("provider_individual_type")
    reason = row.pop("exclusion_sanction_reason")
    # There is one line with multiple dates, we are only going to consider the first one
    startDate = h.parse_date(
        row.pop("exclusion_sanction_effective_date").split(" ")[0],
        formats=["%Y-%m-%d", "%m/%d/%Y"],
    )

    if first_name := row.pop("provider_individual_first_name"):
        person = context.make("Person")
        person.id = context.make_id(first_name, npiCode)
        h.apply_name(
            person,
            first_name=first_name,
            last_name=row.pop("provider_individual_last_name"),
        )
        person.add("country", "us")
        person.add("npiCode", npiCode if npiCode and npiCode.lower() != "n/a" else None)
        person.add("sector", sector)
        person.add("topics", "debarment")
        person_sanction = h.make_sanction(context, person)
        person_sanction.add("reason", reason)
        person_sanction.add("startDate", startDate)

        context.emit(person, target=True)
        context.emit(person_sanction)
    if business_name := row.pop("business_name"):
        company = context.make("Company")
        company.id = context.make_id(business_name, npiCode)
        company.add("name", business_name)
        company.add("country", "us")
        company.add(
            "npiCode", npiCode if npiCode and npiCode.lower() != "n/a" else None
        )
        company.add("sector", sector)
        company.add("topics", "debarment")
        company_sanction = h.make_sanction(context, company)
        company_sanction.add("reason", reason)
        company_sanction.add("startDate", startDate)

        context.emit(company, target=True)
        context.emit(company_sanction)

    if first_name and business_name:
        link = context.make("UnknownLink")
        link.id = context.make_id(person.id, company.id)
        link.add("object", company)
        link.add("subject", person)
        link.add("role", "d/b/a")

        context.emit(link)

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
