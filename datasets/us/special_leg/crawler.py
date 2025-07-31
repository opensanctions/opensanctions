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
    program = row.pop("program")
    entity = context.make(schema)
    entity.id = context.make_slug(name)
    entity.add("topics", topics)
    h.apply_name(entity, name)
    entity.add("alias", row.pop("aliases").split(";"))
    entity.add("country", row.pop("country"))
    entity.add("sourceUrl", source_url.strip())
    entity.add("notes", row.pop("notes"))
    sanction = h.make_sanction(
        context,
        entity,
        program_name=program,
        program_key=h.lookup_sanction_program_key(context, program),
    )
    h.apply_date(sanction, "listingDate", report_date)
    h.apply_date(sanction, "startDate", row.pop("start-date"))
    h.apply_date(sanction, "endDate", row.pop("end-date"))
    sanction.add("reason", row.pop("reason"))
    sanction.add("description", f"Published in {report_date} report.")
    sanction.set("authority", row.pop("authority"))
    sanction.set("sourceUrl", h.multi_split(source_url, ";"))

    context.emit(entity)
    context.emit(sanction)
    context.audit_data(row)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    with open(path, "r") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            crawl_row(context, row)
