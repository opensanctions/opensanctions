from rigour.mime.types import XLSX
from openpyxl import load_workbook
import re

from zavod import Context, helpers as h

AKA_MATCH = r"\(aka ([^)]+)\)"

SKIPROWS = 4


def crawl_item(row: dict[str, str | None], context: Context) -> None:
    provider_name = row.pop("provider_name")
    if not provider_name:
        return

    entity = context.make("LegalEntity")
    entity.id = context.make_id(provider_name, row.get("npi"))

    alias_match = re.search(AKA_MATCH, provider_name)
    if alias_match:
        entity.add("alias", alias_match.group(1))

    provider_name = re.sub(AKA_MATCH, "", provider_name).strip()
    name_parts = h.multi_split(provider_name, ["Owners:", "Owner:"])
    if len(name_parts) > 1:
        entity.add("description", f"Owners: {name_parts[1]}")
    entity.add("name", name_parts[0])
    entity.add("country", "US")

    npi = row.pop("npi")
    if npi and npi != "N/A":
        entity.add("npiCode", npi)

    business_name = row.pop("business_name")
    if business_name and business_name != "N/A":
        entity.add("alias", business_name)

    street = row.pop("street_address")
    city = row.pop("city")
    state = row.pop("state")
    zip_code = row.pop("zip")
    address = h.make_address(
        context,
        street=street,
        city=city,
        state=state,
        postal_code=zip_code,
        country_code="us",
    )
    h.copy_address(entity, address)

    license_number = row.pop("license_number")
    if license_number and license_number != "N/A":
        for ln in h.multi_split(license_number, ["; ", ", "]):
            entity.add("idNumber", ln)

    entity.add("topics", "debarment")
    entity.add("sector", row.pop("provider_type"))

    medicaid_id = row.pop("medicaid_provider_number")
    if medicaid_id and medicaid_id != "N/A":
        entity.add("idNumber", medicaid_id)

    medicare_number = row.pop("medicare_provider_number")
    if medicare_number and medicare_number != "N/A":
        entity.add("idNumber", medicare_number)

    sanction = h.make_sanction(context, entity)
    termination_date = row.pop("exclusion_date")
    assert termination_date is not None
    termination_date = termination_date.replace("Termination ", "")
    termination_date = termination_date.replace("Termination: ", "")
    termination_date = termination_date.replace("Denial ", "").strip()
    h.apply_date(sanction, "startDate", termination_date)
    sanction.add("reason", row.pop("reason_for_exclusion"))
    sanction.add("provisions", row.pop("sanction_type"))

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(
        row,
        ignore=[
            "verification_contact",
            "practice_state",
        ],
    )


def crawl_excel_url(context: Context) -> str:
    doc = context.fetch_html(context.data_url, absolute_links=True)
    xpath = "//a[contains(text(), 'ND Medicaid Provider Exclusion List')][substring(@href, string-length(@href) - 4) = '.xlsx']/@href"
    url = h.xpath_string(doc, xpath)
    assert url is not None, "Could not find Excel file URL"
    return url


def crawl(context: Context) -> None:
    # First we find the link to the excel file
    excel_url = crawl_excel_url(context)
    path = context.fetch_resource("list.xlsx", excel_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)
    assert wb.active is not None

    for item in h.parse_xlsx_sheet(context, wb.active, skiprows=SKIPROWS):
        crawl_item(item, context)
