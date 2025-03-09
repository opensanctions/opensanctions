import csv
from typing import Dict

import zavod.helpers as h
from zavod import Context


def crawl_row(context: Context, row: Dict[str, str]):
    """Process one row of the CSV data"""
    schema = row.pop("schema")
    name = row.pop("name")
    report_date = row.pop("report-date")
    topics = row.pop("topics")
    source_url = row.pop("source_url")
    entity = context.make(schema)
    entity.id = context.make_slug(name)
    entity.add("topics", topics)
    h.apply_name(entity, name)
    entity.add("alias", row.pop("aliases").split(";"))
    entity.add("country", row.pop("country"))
    entity.add("sourceUrl", source_url.strip())
    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "listingDate", report_date)
    h.apply_date(sanction, "endDate", row.pop("end-date"))
    sanction.add("program", row.pop("program"))
    sanction.add("reason", row.pop("reason"))
    sanction.add("description", f"Published in {report_date} report.")
    sanction.set("authority", row.pop("authority"))
    sanction.set("sourceUrl", source_url)

    context.emit(entity)
    context.emit(sanction)
    context.audit_data(row)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    with open(path, "r") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            crawl_row(context, row)
