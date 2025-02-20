from typing import Dict
from rigour.mime.types import XLSX
from openpyxl import load_workbook
import re

from zavod import Context, helpers as h

AKA_MATCH = r"\(aka ([^)]+)\)"


def crawl_item(row: Dict[str, str], context: Context):
    provider_name = row.pop("provider_name")
    if not provider_name:
        return

    entity = context.make("LegalEntity")
    entity.id = context.make_id(provider_name, row.get("n_p_i"))

    alias_match = re.search(AKA_MATCH, provider_name)
    if alias_match:
        entity.add("alias", alias_match.group(1))

    provider_name = re.sub(AKA_MATCH, "", provider_name).strip()
    name_parts = h.multi_split(provider_name, ["Owners:", "Owner:"])
    if len(name_parts) > 1:
        entity.add("description", f"Owners: {name_parts[1]}")
    entity.add("name", name_parts[0])
    entity.add("country", "US")

    npi = row.pop("n_p_i")
    if npi != "N/A":
        entity.add("npiCode", npi)

    address = row.pop("business_nameand_address")
    if address != "N/A":
        entity.add("address", address)

    entity.add("topics", "debarment")
    entity.add("sector", row.pop("provider_type"))
    medicaid_id = row.pop("medicaid_provider_id")
    if medicaid_id and medicaid_id != "N/A":
        entity.add("description", "Medicaid Provider ID: " + medicaid_id)
    medicare_number = row.pop("medicareprovidernumber")
    if medicare_number and medicare_number != "N/A":
        entity.add(
            "description",
            "Medicare Provider Number: " + medicare_number,
        )
    sanction = h.make_sanction(context, entity)
    termination_date = row.pop("exclusiondate")
    termination_date = termination_date.replace("Termination ", "")
    termination_date = termination_date.replace("Termination: ", "")
    termination_date = termination_date.replace("Denial ", "").strip()
    h.apply_date(sanction, "startDate", termination_date)
    sanction.add("reason", row.pop("reason_for_exclusion"))

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(
        row,
        ignore=[
            "providerverification",
            "state",
        ],
    )


def crawl_excel_url(context: Context):
    doc = context.fetch_html(context.data_url)
    doc.make_links_absolute(context.data_url)
    return doc.xpath(
        "//a[@title='ND Medicaid Provider Exclusion List'][substring(@href, string-length(@href) - 4) = '.xlsx']/@href"
    )[0]


def crawl(context: Context) -> None:
    # First we find the link to the excel file
    excel_url = crawl_excel_url(context)
    path = context.fetch_resource("list.xlsx", excel_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)

    for item in h.parse_xlsx_sheet(context, wb.active, skiprows=0):
        crawl_item(item, context)
