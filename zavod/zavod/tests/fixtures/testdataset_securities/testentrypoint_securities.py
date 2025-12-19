import csv
from pathlib import Path
from typing import Dict

from zavod.context import Context

LOCAL_PATH = Path(__file__).parent / "dataset.csv"


def crawl_row(context: Context, row: Dict[str, str]):
    schema = row.pop("type")
    entity = context.make(schema)
    entity.id = context.make_slug(row.pop("id"))
    entity.add("name", row.pop("name"))
    if topics := row.pop("topics"):
        entity.add("topics", topics.split(","))
    if issuer := row.pop("issuer"):
        entity.add("issuer", issuer)
    is_target = row.pop("is_target")
    if is_target == "true":
        entity.add("topics", "sanction")

    context.emit(entity)
    context.audit_data(row)


def crawl(context: Context):
    with open(LOCAL_PATH, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)
