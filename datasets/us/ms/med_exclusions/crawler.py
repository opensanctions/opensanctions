from typing import Dict
import csv

from rigour.mime.types import CSV

from zavod import Context, helpers as h


def crawl_item(row: Dict[str, str], context: Context):
    if not any(row.values()):  # Skip empty rows
        return

    provider_name = row.pop("Provider Name")
    dob = row.pop("Date of Birth")
    npi = row.pop("NPI")

    if dob:
        schema = "Person"
    else:
        schema = "LegalEntity"

    entity = context.make(schema)
    entity.id = context.make_id(provider_name, row.get("npi"))

    entity.add("name", provider_name)
    entity.add("country", "us")
    entity.add("sector", row.pop("Provider Type/Specialty"))
    entity.add("address", row.pop("Provider Address"))
    if entity.schema.name == "Person":
        h.apply_date(entity, "birthDate", dob)
    if npi:
        entity.add("npiCode", npi.split("\n"))

    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "startDate", row.pop("Termination Effective Date"))
    sanction.add("reason", row.pop("Termination Reason"))

    exclusion_period_str = row.pop("Exclusion Period")
    exclusion_period_lookup_result = context.lookup(
        "exclusion_period", exclusion_period_str
    )
    if exclusion_period_lookup_result:
        sanction.add("endDate", exclusion_period_lookup_result.end_date)
    else:
        exclusion_period_segments = h.multi_split(exclusion_period_str, ["-"])
        # Only apply if it looks like "start_date - end_date"
        if len(exclusion_period_segments) == 2:
            _, end_date_str = exclusion_period_segments
            if end_date_str.lower() != "indefinite":
                h.apply_date(sanction, "endDate", end_date_str)
        else:
            # You will likely want to add an exclusion_period lookup
            context.log.warning(
                'Exclusion period does not look like "start_date - end_date"',
                exclusion_period=exclusion_period_str,
            )

    if h.is_active(sanction):
        entity.add("topics", "debarment")

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row, ignore=["Sanction Type", ""])


def crawl_data_url(context: Context):
    doc = context.fetch_html(context.data_url)
    doc.make_links_absolute(context.data_url)
    return doc.xpath("//a[contains(text(), 'Sanctioned Provider List')]/@href")[0]


def crawl(context: Context) -> None:
    data_url = crawl_data_url(context)
    path = context.fetch_resource("source.csv", data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)

    with open(path, "r", encoding="latin-1") as f:
        lines = f.readlines()

    # Skip the first 8 lines
    for row in csv.DictReader(lines[9:]):
        crawl_item(row, context)
