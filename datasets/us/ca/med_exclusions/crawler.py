import csv

from normality import slugify
from rigour.ids.npi import NPI
from rigour.mime.types import CSV
from zavod.extract.zyte_api import fetch_html

from zavod import Context
from zavod import helpers as h


def crawl_item(row: dict[str, str], context: Context) -> None:
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
    entity.add("sector", h.multi_split(row.pop("provider_type"), [";"]))
    entity.add("address", h.multi_split(addresses, [", &", ";"]))
    entity.add(
        "registrationNumber", h.multi_split(row.pop("license_number"), [",", ";"])
    )

    for num in h.multi_split(row.pop("provider_number"), [","]):
        if NPI.is_valid(num):
            entity.add("npiCode", num)
        else:
            entity.add("registrationNumber", h.multi_split(num, [",", ";"]))

    sanction = h.make_sanction(context, entity)
    start_date = row.pop("date_of_suspension")
    if start_date != "N/A":
        h.apply_date(sanction, "startDate", start_date)
    sanction.add("duration", row.pop("active_period"))

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def crawl_data_url(context: Context) -> str:
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
    dataset_url = h.xpath_string(landing_doc, xpath + "/@href")
    assert dataset_url is not None, "Could not find dataset URL"

    dataset_doc = context.fetch_html(dataset_url, cache_days=1, absolute_links=True)
    resource_url = h.xpath_strings(dataset_doc, xpath + "/@href")[0]
    assert resource_url is not None, "Could not find resource URL"

    resource_doc = context.fetch_html(resource_url, cache_days=1, absolute_links=True)
    file_xpath = ".//a[contains(@href, '.csv')]"
    file_url = h.xpath_string(resource_doc, file_xpath + "/@href")
    assert file_url is not None, "Could not find CSV file URL"
    return file_url


def crawl(context: Context) -> None:
    # First we find the link to the excel file
    excel_url = crawl_data_url(context)
    path = context.fetch_resource("source.csv", excel_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)

    with open(path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        assert reader.fieldnames is not None
        reader.fieldnames = [slugify(key, sep="_") or "" for key in reader.fieldnames]
        for row in reader:
            crawl_item(row, context)
