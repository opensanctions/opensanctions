from itertools import product
from typing import Dict
import re
from normality import slugify
from rigour.mime.types import CSV
import csv


from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_html, fetch_resource

REGEX_DBA = re.compile(r"\bdba\b", re.IGNORECASE)
REGEX_AKA = re.compile(r"\(?a\.?k\.?a\b\.?|\)", re.IGNORECASE)


def crawl_item(row: Dict[str, str], context: Context):
    if row.pop(" Type of Exclusion") == "OIG":
        return

    raw_last_entity_name = row.pop(" Last Name or Entity Name")
    npi = row.pop(" NPI#").strip()
    address = row.pop(" State and Zip")
    birth_date = row.pop(" Birthdate")

    if raw_first_name := row.pop("First Name").strip():
        entity = context.make("Person")
        entity.id = context.make_id(
            raw_first_name, raw_last_entity_name, birth_date, address, npi
        )

        first_names = REGEX_AKA.split(raw_first_name)
        last_names = REGEX_AKA.split(raw_last_entity_name)

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
            h.apply_name(
                entity, first_name=raw_first_name, last_name=raw_last_entity_name
            )
        h.apply_date(entity, "birthDate", birth_date)
    else:
        entity = context.make("Company")
        entity.id = context.make_id(raw_last_entity_name, address, npi)

        names = REGEX_DBA.split(raw_last_entity_name)
        entity.add("name", names[0])
        entity.add("alias", names[1:])

    if affiliate_name := row.pop(" Affiliated Entity").strip():
        affiliate = context.make("LegalEntity")
        affiliate.id = context.make_id(affiliate_name)
        if slugify(affiliate) != slugify(raw_last_entity_name):
            affiliate.add("name", affiliate_name)
            link = context.make("UnknownLink")
            link.id = context.make_id("link", entity.id, affiliate.id)
            link.add("object", entity)
            link.add("subject", affiliate)
            link.add("role", "Affiliated")
            context.emit(affiliate)
            context.emit(link)

    entity.add("country", "us")
    entity.add("sector", row.pop(" Title or Provider Type"))
    entity.add("address", address)

    if npi != "NRF":
        entity.add("npiCode", npi)
    sanction = h.make_sanction(context, entity)
    sanction.add("reason", row.pop(" Reason for Exclusion"))
    sanction.add("reason", row.pop(" Reason for Termination"))
    sanction.add("duration", row.pop(" Period of Exclusion"))
    sanction.add("duration", row.pop(" Period of Enrollment Prohibition"))
    h.apply_date(sanction, "startDate", row.pop(" Effective Date"))

    if (reinstate := row.pop(" Reinstate")) and ("9999" not in reinstate):
        h.apply_date(sanction, "endDate", reinstate)

    is_debarred = h.is_active(sanction)
    if is_debarred:
        entity.add("topics", "debarment")

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(
        row, ignore=[" Program Office", " Period of Enrollment Prohibition"]
    )


def crawl_csv_url(context: Context):
    file_xpath = ".//a[contains(text(), 'Download CSV')]"
    doc = fetch_html(context, context.data_url, file_xpath)
    doc.make_links_absolute(context.data_url)
    return doc.xpath(file_xpath)[0].get("href")


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
