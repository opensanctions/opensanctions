import csv
from pathlib import Path
from typing import Dict
from pantomime.types import CSV

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

    context.emit(entity, target=row.pop("is_target"))
    context.audit_data(row)


def crawl(context: Context):
    data_path = context.get_resource_path("source.csv")
    with open(LOCAL_PATH, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)
