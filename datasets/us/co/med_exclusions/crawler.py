from typing import Dict
from rigour.mime.types import XLSX
from openpyxl import load_workbook
from datetime import datetime

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_html


def crawl_item(row: Dict[str, str], context: Context):

    name = row.pop("provider_name")

    if not name:
        return

    entity = context.make("LegalEntity")
    entity.id = context.make_id(name, row.get("npi"))
    entity.add("name", name)
    entity.add("country", "us")

    if row.get("npi") != "N/A":
        npis = h.multi_split(row.pop("npi"), ["; ", "&", " and "])

        entity.add("npiCode", npis)
    else:
        row.pop("npi")

    sanction = h.make_sanction(context, entity)
    termination_effective_date = row.pop("termination_effective_date")
    sanction.add("startDate", termination_effective_date)
    sanction.add("reason", row.pop("termination_authority"))

    reinstatement_date = row.pop("reinstatement_effective_date", None)

    if reinstatement_date:
        target = datetime.strptime(reinstatement_date, "%Y-%m-%d") >= datetime.today()
        sanction.add("endDate", reinstatement_date)
    else:
        target = True

    if target:
        entity.add("topics", "debarment")

    dba = row.pop("doing_business_as_name")
    if dba:
        company = context.make("Company")
        company.id = context.make_id(dba)
        company.add("name", dba)
        company.add("country", "us")

        if target:
            company.add("topics", "debarment")

        link = context.make("UnknownLink")
        link.id = context.make_id(entity.id, company.id)
        link.add("object", entity)
        link.add("subject", company)
        link.add("role", "d/b/a")

        company_sanction = h.make_sanction(context, company)
        company_sanction.add("startDate", termination_effective_date)
        if reinstatement_date:
            company_sanction.add("endDate", reinstatement_date)

        context.emit(company, target=target)
        context.emit(company_sanction)
        context.emit(link)

    context.emit(entity, target=target)
    context.emit(sanction)

    context.audit_data(row)


def unblock_validator(doc) -> bool:
    return bool(doc.find(".//*[@about='/provider-termination']"))


def crawl_excel_url(context: Context):
    doc = fetch_html(context, context.data_url, unblock_validator=unblock_validator)
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
