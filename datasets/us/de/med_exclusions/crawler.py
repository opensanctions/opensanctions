from typing import Dict
from rigour.mime.types import PDF
import re

from zavod import Context, helpers as h, settings
from zavod.shed.zyte_api import fetch_resource

REGEX_AKA = re.compile(r"\baka\b|a\.k\.a\.?", re.IGNORECASE)
REGEX_DBA = re.compile(r"d\.b\.a.|\bdba\b", re.IGNORECASE)
# Ruth Diane Jones, DO
# Tamika Brewer-Adams (PCA)
# Charles Michael Napoli (Pharma Tech)
REGEX_JOB_ROLE = re.compile(r"^(?P<name>.+)[,\s]+(?P<role>([A-Z\.,/-]+|\([^\)]+\)))$")


def crawl_item(row: Dict[str, str], context: Context):
    raw_name = row.pop("sanctioned_provider_name")
    names = REGEX_AKA.split(raw_name)
    name, alias = names[0], names[1:]
    position = None
    if match := REGEX_JOB_ROLE.match(name):
        if match.group("role").lower() not in {"llc", "inc", "corp", "pc"}:
            position = match.group("role")
            name = match.group("name").rstrip(",")

    npi = row.pop("npi")

    entity = context.make("LegalEntity")
    entity.id = context.make_id(raw_name, npi)

    if "d.b.a." in name:
        names = REGEX_DBA.split(name)
        name, dba = names[0], names[1]

        dba_company = context.make("Company")
        dba_company.id = context.make_id(dba)
        dba_company.add("name", dba)
        dba_company.add("country", "us")
        dba_company.add("topics", "debarment")
        link = context.make("UnknownLink")
        link.id = context.make_id(entity.id, dba_company.id)
        link.add("object", dba_company)
        link.add("subject", entity)
        link.add("role", "d/b/a")
        context.emit(dba_company)
        context.emit(link)

    entity.add("name", name)
    entity.add_cast("Person", "position", position)
    entity.add("alias", alias)
    entity.add("country", "us")
    entity.add("sector", row.pop("taxonomy"))
    entity.add("idNumber", row.pop("dea"))
    entity.add("npiCode", h.multi_split(npi, [";", ",", "&"]))
    license_number = row.pop("license")
    if license_number != "N/A":
        entity.add("idNumber", license_number.split(","))

    sanction = h.make_sanction(context, entity)
    sanction.add("description", row.pop("comments"))
    sanction.set("authority", row.pop("oig_medicaid_sanction"))
    h.apply_date(sanction, "startDate", row.pop("effective_date"))
    reinstated_date = row.pop("reinstated_date")
    h.apply_date(sanction, "endDate", reinstated_date)

    end_date = sanction.get("endDate")
    if end_date:
        is_debarred = max(end_date) >= settings.RUN_TIME_ISO
    else:
        res = context.lookup("type.date", reinstated_date)
        if res and res.is_debarred is not None:
            is_debarred = res.is_debarred
        else:
            is_debarred = True
            context.log.warn(
                "No `is_debarred` for end date mapping", reinstated=reinstated_date
            )

    if is_debarred:
        entity.add("topics", "debarment")

    context.emit(entity, target=is_debarred)
    context.emit(sanction)

    context.audit_data(row, ignore=["year"])


def crawl(context: Context) -> None:
    _, _, _, path = fetch_resource(
        context, "source.pdf", context.data_url, expected_media_type=PDF
    )
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)
    for item in h.parse_pdf_table(context, path, headers_per_page=True):
        crawl_item(item, context)
