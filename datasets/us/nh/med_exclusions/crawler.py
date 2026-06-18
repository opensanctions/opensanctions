from openpyxl import load_workbook
from zavod.extract import zyte_api

from zavod import Context
from zavod import helpers as h


def crawl_item(row: dict[str, str | None], context: Context) -> None:
    first_name = row.pop("provider_individual_first_name")
    business_name = row.pop("business_name")
    npi = row.pop("national_provider_identifier_npi")
    sector = row.pop("provider_individual_type")
    reason = row.pop("exclusion_sanction_reason")
    # There is one line with multiple dates, we are only going to consider the first one
    startDate = (row.pop("exclusion_sanction_effective_date") or "").split(" ")[0]

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


def crawl_excel_url(context: Context) -> str:
    sanction_list_xpath = (
        "//*[contains(text(), 'Provider Exclusion and Sanctions List')]"
    )
    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        unblock_validator=sanction_list_xpath,
        absolute_links=True,
    )
    url = h.xpath_string(doc, sanction_list_xpath + "/@href")
    assert url is not None, "Could not find Excel file URL"
    return url


def crawl(context: Context) -> None:
    # First we find the link to the excel file
    excel_url = crawl_excel_url(context)

    _, _, _, path = zyte_api.fetch_resource(context, "source.xlsx", excel_url)
    context.export_resource(path, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)

    assert wb.active is not None
    for item in h.parse_xlsx_sheet(context, wb.active, skiprows=1):
        crawl_item(item, context)
