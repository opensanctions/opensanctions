import csv
from typing import Dict, List, Optional, Set, Tuple
from banal import hash_data

from zavod import Context
import zavod.helpers as h


def crawl_row(context: Context, row: Dict[str, str]):
    """Process one row of the CSV data"""
    row_id = row.pop("List ID").strip(" \t.")
    entity_type = row.pop("Type").strip()
    name = row.pop("Name").strip()
    country = row.pop("Country").strip()

    # context.log.info(f"Processing row ID {row_id}: {name}")
    entity = context.make(entity_type)
    entity.id = context.make_id(row_id, name, country)
    context.log.debug(f"Unique ID {entity.id}")
    entity.add("topics", "sanction")
    entity.add("country", country)
    entity.add("sourceUrl", row.pop("Source URL", None))
    entity.add("birthDate", row.pop("DOB", None))
    h.apply_name(entity, name)
    alias = row.pop("Alias").strip()
    if alias:
        h.apply_name(entity, alias, alias=True)
    context.audit_data(row)
    sanction = h.make_sanction(context, entity)
    context.emit(entity, target=True)
    context.emit(sanction)


def crawl_csv(context: Context):
    """Process the CSV data"""
    path = context.fetch_resource("reg_2878_database.csv", context.data_url)
    with open(path, "rt") as infh:
        reader = csv.DictReader(infh)
        for row in reader:
            crawl_row(context, row)


def crawl(context: Context):
    crawl_csv(context)
