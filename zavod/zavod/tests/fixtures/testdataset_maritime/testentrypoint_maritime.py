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
    entity.add("imoNumber", row.pop("imo"))
    if topics := row.pop("topics"):
        entity.add("topics", topics.split(","))

    context.emit(entity)
    context.audit_data(row)


def crawl(context: Context):
    with open(LOCAL_PATH, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)
