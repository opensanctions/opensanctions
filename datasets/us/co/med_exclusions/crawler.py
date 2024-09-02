from typing import Dict
from rigour.mime.types import XLSX
from openpyxl import load_workbook
from datetime import datetime
import re

from zavod import Context, helpers as h


def crawl_item(row: Dict[str, str], context: Context):

    name = row.pop("provider_name")

    if not name:
        return

    entity = context.make("LegalEntity")
    entity.id = context.make_id(name, row.get("npi"))
    entity.add("name", name)

    if row.get("npi") != "N/A":
        npis = re.split(r"; |& ", row.pop("npi"))

        for npi in npis:
            entity.add("npiCode", npi)
    else:
        row.pop("npi")

    sanction = h.make_sanction(context, entity)
    termination_effective_date = row.pop("termination_effective_date")
    sanction.add("startDate", termination_effective_date)
    sanction.add("reason", row.pop("termination_authority"))

    if "reinstatement_effective_date" in row:
        reinstatement_effective_date = row.pop("reinstatement_effective_date")
    else:
        reinstatement_effective_date = None

    if reinstatement_effective_date:
        target = (
            datetime.strptime(reinstatement_effective_date, "%Y-%m-%d")
            >= datetime.today()
        )
        sanction.add("endDate", reinstatement_effective_date)
    else:
        target = True

    if target:
        entity.add("topics", "debarment")

    dba = row.pop("doing_business_as_name")
    if dba:
        company = context.make("Company")
        company.id = context.make_id(dba)

        if target:
            company.add("topics", "debarment")

        # link = context.make("UnknownLink")
        # link.id = context.make_id(dba)
        # link.add("object", entity)
        # link.add("subject", company)
        # link.add("role", "d/b/a")

        company_sanction = h.make_sanction(context, company)
        company_sanction.add("startDate", termination_effective_date)
        if reinstatement_effective_date:
            company_sanction.add("endDate", reinstatement_effective_date)

        context.emit(company, target=target)
        context.emit(company_sanction)
        # context.emit(link)

    context.emit(entity, target=target)
    context.emit(sanction)

    context.audit_data(row)


def crawl_excel_url(context: Context):
    doc = context.fetch_html(context.data_url)
    return doc.find(".//*[@about='/provider-termination']").find(".//a").get("href")


def crawl(context: Context) -> None:
    # First we find the link to the excel file
    excel_url = crawl_excel_url(context)
    path = context.fetch_resource("list.xlsx", excel_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)

    # Currently terminated providers
    for item in h.parse_xlsx_sheet(context, wb["Termination List"]):
        crawl_item(item, context)

    # Providers that where terminated but are now reinstated
    for item in h.parse_xlsx_sheet(context, wb["Reinstated Providers"]):
        crawl_item(item, context)
