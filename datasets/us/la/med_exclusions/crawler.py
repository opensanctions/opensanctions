from typing import Dict
import re
from rigour.mime.types import CSV
import csv

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_html, fetch_resource

DBA_PATTERN = r"(.+)\s+dba\s+(.+)"
AKA_PATTERN = r"(.+?)\s*(?:\(\s*aka\s*\s*\w+\s*\)|aka|AKA|,\s*AKA)\s*(.+)"


def crawl_item(row: Dict[str, str], context: Context):

    if row.pop(" Type of Exclusion") != "State":
        return

    if raw_first_name := row.pop("First Name"):
        raw_last_name = row.pop(" Last Name or Entity Name")

        aka_match_first = re.search(AKA_PATTERN, raw_first_name, re.IGNORECASE)
        aliases = []
        if aka_match_first:
            first_name, alias = (
                aka_match_first.group(1).strip(),
                aka_match_first.group(2).strip(),
            )
            aliases.append(alias)
        else:
            first_name = raw_first_name

        aka_match_last = re.search(AKA_PATTERN, raw_last_name, re.IGNORECASE)
        if aka_match_last:
            last_name, alias = (
                aka_match_last.group(1).strip(),
                aka_match_last.group(2).strip(),
            )
            aliases.append(alias)
        else:
            last_name = raw_last_name
        entity = context.make("Person")
        entity.id = context.make_id(raw_first_name, raw_last_name)
        h.apply_name(
            entity,
            first_name=first_name,
            last_name=last_name,
        )
        h.apply_date(entity, "birthDate", row.pop(" Birthdate"))
        entity.add("alias", aliases)
    else:
        raw_name = row.pop(" Last Name or Entity Name")
        dba_match = re.search(DBA_PATTERN, raw_name, re.IGNORECASE)

        if dba_match:
            name, dba_company_name = (
                dba_match.group(1).strip(),
                dba_match.group(2).strip(),
            )
        else:
            name = raw_name.strip()
            dba_company_name = None

        entity = context.make("Company")
        entity.id = context.make_id(raw_name)
        entity.add("name", name)

        if dba_company_name:
            dba_company = context.make("Company")
            dba_company.id = context.make_id(dba_company_name)
            dba_company.add("name", dba_company_name)
            link = context.make("UnknownLink")
            link.id = context.make_id(entity.id, dba_company.id)
            link.add("object", dba_company)
            link.add("subject", entity)
            link.add("role", "d/b/a")
            context.emit(dba_company)
            context.emit(link)

    if row.get(" Affiliated Entity"):
        affiliated = context.make("LegalEntity")
        affiliated.id = context.make_id(row.get(" Affiliated Entity"))
        affiliated.add("name", row.pop(" Affiliated Entity"))
        link = context.make("UnknownLink")
        link.id = context.make_id(entity.id, affiliated.id)
        link.add("object", entity)
        link.add("subject", affiliated)
        link.add("role", "Affiliated")
        context.emit(affiliated)
        context.emit(link)

    entity.add("country", "us")
    entity.add("sector", row.pop(" Title or Provider Type"))
    entity.add("topics", "debarment")
    entity.add("address", row.pop(" State and Zip"))

    if row.get(" NPI#") and row.get(" NPI#") != "NRF":
        entity.add("npiCode", row.pop(" NPI#"))
    else:
        row.pop(" NPI#")
    sanction = h.make_sanction(context, entity)
    sanction.add("reason", row.pop(" Reason for Exclusion"))
    sanction.add("duration", row.pop(" Period of Exclusion"))
    h.apply_date(sanction, "startDate", row.pop(" Effective Date"))

    if row.get(" Reinstate"):
        h.apply_date(sanction, "endDate", row.pop(" Reinstate"))
        is_debarred = False
    else:
        row.pop(" Reinstate")
        is_debarred = True

    if is_debarred:
        entity.add("topics", "debarment")

    context.emit(entity, target=is_debarred)
    context.emit(sanction)

    context.audit_data(
        row, ignore=[" Program Office", " Period of Enrollment Prohibition"]
    )


def unblock_validator(doc) -> bool:
    return len(doc.xpath(".//a[contains(text(), 'Download CSV')]")) > 0


def crawl_csv_url(context: Context):
    doc = fetch_html(context, context.data_url, unblock_validator=unblock_validator)
    doc.make_links_absolute(context.data_url)
    return doc.xpath(".//a[contains(text(), 'Download CSV')]")[0].get("href")


def crawl(context: Context) -> None:

    csv_url = crawl_csv_url(context)

    cached, path, mediatype, _charset = fetch_resource(
        context, "source.csv", csv_url, geolocation="US"
    )
    if not cached:
        assert mediatype == CSV
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)

    with open(path) as f:
        next(f)
        for item in csv.DictReader(f):
            crawl_item(item, context)
