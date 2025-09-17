import csv
from typing import Dict
from normality import slugify
from rigour.mime.types import CSV
from rigour.ids.npi import NPI

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_html


def crawl_item(row: Dict[str, str], context: Context):
    addresses = row.pop("address_es")
    first_name = row.pop("first_name")
    middle_name = row.pop("middle_name")
    last_name = row.pop("last_name")
    aliases = row.pop("a_k_a_also_known_asd_b_a_doing_business_as")

    if first_name != "N/A":
        entity = context.make("Person")
        entity.id = context.make_id(first_name, middle_name, last_name, addresses)

        h.apply_name(
            entity,
            first_name=first_name,
            middle_name=middle_name if middle_name != "N/A" else None,
            last_name=last_name,
        )
        if aliases != "N/A":
            entity.add("alias", [a.strip() for a in aliases.split(";")])

    else:
        entity = context.make("Company")
        entity.id = context.make_id(last_name, addresses)
        entity.add("name", last_name)

        if aliases != "N/A":
            for alias in aliases.split(";"):
                related_entity = context.make("LegalEntity")
                related_entity.id = context.make_id(alias, entity.id)
                related_entity.add("name", alias.strip())
                related_entity.add("country", "us")
                related_entity.add("topics", "debarment")
                rel = context.make("UnknownLink")
                rel.id = context.make_id(entity.id, related_entity.id)
                rel.add("subject", entity)
                rel.add("object", related_entity)
                context.emit(related_entity)
                context.emit(rel)

    entity.add("country", "us")
    entity.add("topics", "debarment")
    entity.add("sector", row.pop("provider_type"))
    entity.add("address", h.multi_split(addresses, [", &", ";"]))
    entity.add("registrationNumber", row.pop("license_number").split(", "))

    for num in h.multi_split(row.pop("provider_number"), [","]):
        if NPI.is_valid(num):
            entity.add("npiCode", num)
        else:
            entity.add("registrationNumber", num)

    sanction = h.make_sanction(context, entity)
    start_date = row.pop("date_of_suspension")
    if start_date != "N/A":
        h.apply_date(sanction, "startDate", start_date)
    sanction.add("duration", row.pop("active_period"))

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def crawl_data_url(context: Context):
    # Landing page
    xpath = ".//a[contains(text(), 'Suspended')][contains(text(), 'Ineligible')][contains(text(), 'List')]"

    landing_doc = fetch_html(
        context,
        context.data_url,
        xpath,
        geolocation="US",
        cache_days=1,
        absolute_links=True,
    )
    dataset_url = landing_doc.xpath(xpath)[0].get("href")

    dataset_doc = context.fetch_html(dataset_url, cache_days=1, absolute_links=True)
    resource_url = dataset_doc.xpath(xpath)[0].get("href")

    resource_doc = context.fetch_html(resource_url, cache_days=1, absolute_links=True)
    file_xpath = ".//a[contains(@href, '.csv')]"
    file_url = resource_doc.xpath(file_xpath)[0].get("href")
    return file_url


def crawl(context: Context) -> None:
    # First we find the link to the excel file
    excel_url = crawl_data_url(context)
    path = context.fetch_resource("source.csv", excel_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)

    with open(path, "r") as f:
        reader = csv.DictReader(f)
        reader.fieldnames = [slugify(key, sep="_") for key in reader.fieldnames]
        for row in reader:
            crawl_item(row, context)
