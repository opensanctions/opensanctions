from itertools import product
from typing import Dict
import re
from rigour.mime.types import CSV
import csv

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_html, fetch_resource

REGEX_DBA = re.compile(r"\bdba\b", re.IGNORECASE)
REGEX_AKA = re.compile(r"\(?a\.?k\.?a\b\.?|\)", re.IGNORECASE)


def crawl_item(row: Dict[str, str], context: Context):
    if row.pop(" Type of Exclusion") == "OIG":
        return

    if raw_first_name := row.pop("First Name"):
        raw_last_name = row.pop(" Last Name or Entity Name")

        entity = context.make("Person")
        entity.id = context.make_id(raw_first_name, raw_last_name)

        first_names = REGEX_AKA.split(raw_first_name)
        last_names = REGEX_AKA.split(raw_last_name)

        for first_name, last_name in product(first_names, last_names):
            first_name = first_name.strip()
            last_name = last_name.strip()
            if not first_name or not last_name:
                continue
            h.apply_name(
                entity,
                first_name=first_name,
                last_name=last_name,
            )
        if not entity.has("name"):
            h.apply_name(entity, first_name=raw_first_name, last_name=raw_last_name)
        h.apply_date(entity, "birthDate", row.pop(" Birthdate"))
    else:
        raw_name = row.pop(" Last Name or Entity Name")

        entity = context.make("Company")
        entity.id = context.make_id(raw_name)

        names = REGEX_DBA.split(raw_name)
        entity.add("name", names[0])
        entity.add("alias", names[1:])
        row.pop(" Birthdate")

    if affiliate := row.pop(" Affiliated Entity").strip():
        affiliated = context.make("LegalEntity")
        affiliated.id = context.make_id(affiliate)
        affiliated.add("name", affiliate)
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
    sanction.add("reason", row.pop(" Reason for Termination"))
    sanction.add("duration", row.pop(" Period of Exclusion"))
    sanction.add("duration", row.pop(" Period of Enrollment Prohibition"))
    h.apply_date(sanction, "startDate", row.pop(" Effective Date"))

    if (reinstate := row.pop(" Reinstate")) and ("9999" not in reinstate):
        h.apply_date(sanction, "endDate", reinstate)
        is_debarred = False
    else:
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
    _, _, _, path = fetch_resource(
        context,
        "source.csv",
        csv_url,
        expected_media_type=CSV,
        geolocation="US",
    )
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)

    with open(path) as f:
        f.readline()  # Skip the date row

        for item in csv.DictReader(f):
            crawl_item(item, context)
