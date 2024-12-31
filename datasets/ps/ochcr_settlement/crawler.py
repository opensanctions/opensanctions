"""
Create list of OHCHR companies linked to illegal settlements from CSV.
"""

import csv
from typing import Dict, Iterable

import zavod.helpers as h
from zavod import Context


def crawl_row(context: Context, row: Dict[str, str]):
    """Process one row of the CSV data"""
    row_id = row.pop("ID").strip(" \t.")
    name = row.pop("Business enterprise").strip()
    prev_name = row.pop("Previous name").strip()
    notes = row.pop("Notes").strip()
    activities = row.pop("Sub-paragraph of listed activity (2020 report)").strip()
    country = row.pop("State concerned").strip()
    section = row.pop("Section").strip()
    date = row.pop("Date")
    # FIXME: maybe that trailing space will go away?
    if "Source URL" in row:
        source_url = row.pop("Source URL").strip()
    else:
        source_url = row.pop("Source URL").strip()
    context.log.info(f"Processing row ID {row_id}: {name}")
    context.audit_data(row)
    if section.startswith("A. ") or "no longer involved" in section:
        context.log.info(
            f"Skipping company {name} as marked as "
            f"not involved (Section: {section})"
        )
        return
    entity = context.make("Company")
    entity.id = context.make_id(name, country)
    context.log.debug(f"Unique ID {entity.id}")
    h.apply_name(entity, name)
    if prev_name:
        entity.add("previousName", prev_name)
    entity.add("program", section)
    entity.add("notes", f"Listed activities: {activities}")
    entity.add("notes", f"Date of last update: {date}")
    entity.add("topics", "debarment")
    if notes:
        entity.add("notes", notes)
    entity.add("sourceUrl", source_url)
    context.emit(entity, target=True)


def crawl_csv(context: Context, reader: Iterable[Dict[str, str]]):
    """Process the CSV data"""
    for row in reader:
        crawl_row(context, row)


def crawl(context: Context):
    """Crawl the OHCHR database as converted to CSV"""
    path = context.fetch_resource("ohchr_database.csv", context.data_url)
    with open(path, "rt") as infh:
        reader = csv.DictReader(infh)
        crawl_csv(context, reader)
