"""
Add supplemental list of EU sanctions from annex in Regulation  2023/2878
"""

import csv
from typing import Dict, Iterable

import zavod.helpers as h
from zavod import Context


def crawl_row(context: Context, row: Dict[str, str]):
    """Process one row of the CSV data"""
    row_id = row.pop("List ID").strip(" \t.")
    entity_type = row.pop("Type").strip()
    name = row.pop("Name").strip()
    country = row.pop("Country").strip()

    context.log.info(f"Processing row ID {row_id}: {name}")
    entity = context.make(entity_type)
    entity.id = context.make_id(row_id, name, country)
    context.log.debug(f"Unique ID {entity.id}")
    entity.add("topics", "sanction")
    entity.add("country", country)
    h.apply_name(entity, name)
    alias = row.pop("Alias").strip()
    if alias:
        h.apply_name(entity, alias, alias=True)
    entity.add("sourceUrl", row.pop("Source URL").strip())
    context.audit_data(row)
    sanction = h.make_sanction(context, entity)
    context.emit(entity, target=True)
    context.emit(sanction)


def crawl_csv(context: Context, reader: Iterable[Dict[str, str]]):
    """Process the CSV data"""
    for row in reader:
        crawl_row(context, row)


def crawl(context: Context):
    """Crawl the OHCHR database as converted to CSV"""
    path = context.fetch_resource("reg_2878_database.csv", context.data_url)
    with open(path, "rt") as infh:
        reader = csv.DictReader(infh)
        crawl_csv(context, reader)
