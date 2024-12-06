from typing import Dict
from rigour.mime.types import PDF
from datetime import datetime
import re

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_resource

DATE_PATTERN = r"\b\d{1,2}/\d{1,2}/\d{4}\b"
REGEX_AKA = re.compile(r"\(?a\.?k\.?a\b\.?|\)", re.IGNORECASE)
REGEX_DBA = re.compile(r"d.b.a.", re.IGNORECASE)


def crawl_item(row: Dict[str, str], context: Context):

    raw_name = row.pop("sanctioned_provider_name")

    entity = context.make("LegalEntity")
    entity.id = context.make_id(raw_name, row.get("npi"))

    names = REGEX_AKA.split(raw_name)
    name, alias = names[0], names[1:]

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
    entity.add("alias", alias)
    entity.add("country", "us")

    npi = row.pop("npi")
        if npi != "N/A":
            entity.add(
            "npiCode",
            h.multi_split(
                npi,
                [";", ",", "&"],
            ),
        )

    if row.get("license") != "N/A":
        entity.add("idNumber", row.pop("license").split(","))
    else:
        row.pop("license")

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", row.pop("effective_date"))
    sanction.add("description", row.pop("comments"))
    sanction.set("authority", row.pop("oig_medicaid_sanction"))

h.apply_date(sanction, "endDate", row.pop("reinstated_date"))
end_date = max(sanction.get('endDate'), None)
if end_date is not None: 
      target = datetime.strptime(reinstatement_date, "%Y-%m-%d") >= settings.RUN_TIME_ISO
else:
         target = True
    context.emit(entity, target=target)
    context.emit(sanction)

    context.audit_data(row, ignore=["year", "taxonomy", "dea"])


def crawl(context: Context) -> None:
    _, _, _, path = fetch_resource(
        context, "source.pdf", context.data_url, expected_media_type=PDF
    )
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)

    for item in h.parse_pdf_table(context, path, headers_per_page=True):
        crawl_item(item, context)
