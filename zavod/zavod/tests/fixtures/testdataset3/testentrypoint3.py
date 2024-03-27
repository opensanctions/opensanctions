from pathlib import Path
from typing import Dict
import csv

from zavod.context import Context

LOCAL_PATH = Path(__file__).parent / "dataset.csv"


def crawl_row(context: Context, row: Dict[str, str]):
    entity = context.make("Company")
    id = row.pop("id")
    entity.id = context.make_slug(id)
    entity.add("name", row.pop("name"))
    entity.add("topics", row.pop("topics") or None)
    entity.add("country", row.pop("country"))
    parent_id = row.pop("parent_id") or None
    if parent_id:
        entity.add("parent", context.make_slug(parent_id))
    owner_id = row.pop("owner_id") or None
    if owner_id:
        ownership = context.make("Ownership")
        ownership.id = context.make_slug(id, "ownership", owner_id)
        ownership.add("asset", entity)
        ownership.add("owner", context.make_slug(owner_id))
        context.emit(ownership)
    context.emit(entity, target=row.pop("is_target") == "true")
    context.audit_data(row)


def crawl(context: Context):
    with open(LOCAL_PATH, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)
