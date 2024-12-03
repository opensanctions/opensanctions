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
    h.apply_dates(entity, "birthDate", h.multi_split(row.pop("DOB"), ";"))
    entity.add("country", h.multi_split(country, ";"))
    entity.add("name", h.multi_split(name, ";"))
    entity.add("alias", h.multi_split(row.pop("Alias"), ";"))
    entity.add_cast("Person", "passportNumber", h.multi_split(row.pop("passport"), ";"))
    entity.add("taxNumber", h.multi_split(row.pop("taxNumber"), ";"))
    entity.add("registrationNumber", h.multi_split(row.pop("registrationNumber"), ";"))
    entity.add("idNumber", h.multi_split(row.pop("idNumber"), ";"))
    entity.add("notes", row.pop("Notes").strip())
    entity.add("sourceUrl", h.multi_split(row.pop("Source URL"), ";"))

    for related_name in h.multi_split(row.pop("related"), ";"):
        related = context.make("LegalEntity")
        related.id = context.make_id(related_name, entity.id)
        related.add("name", related_name)

        rel = context.make("UnknownLink")
        rel.id = context.make_id(related.id, entity.id)
        rel.add("subject", related)
        rel.add("object", entity)

        context.emit(related)
        context.emit(rel)

    sanction = h.make_sanction(context, entity)

    context.emit(entity, target=True)
    context.emit(sanction)

    context.audit_data(row)


def crawl_csv(context: Context, reader: Iterable[Dict[str, str]]):
    """Process the CSV data"""
    for row in reader:
        crawl_row(context, row)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    with open(path, "rt") as infh:
        reader = csv.DictReader(infh)
        crawl_csv(context, reader)
