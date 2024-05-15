import csv
from typing import Dict

import zavod.helpers as h
from zavod import Context


def crawl_row(context: Context, row: Dict[str, str]):
    """Process one row of the CSV data"""
    name = row.pop("name")
    entity = context.make("Company")
    entity.id = context.make_slug(name)
    entity.add("topics", row.pop("topics").split(";"))
    h.apply_name(entity, name)
    entity.add("alias", row.pop("aliases").split(";"))
    entity.add("sourceUrl", row.pop("source_url").strip())
    sanction = h.make_sanction(context, entity)
    sanction.add("program", row.pop("program"))
    context.emit(entity, target=True)
    context.emit(sanction)
    context.audit_data(row)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    with open(path, "r") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            crawl_row(context, row)
