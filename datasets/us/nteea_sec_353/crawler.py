from pantomime.types import CSV
from typing import Dict
import csv

from zavod import Context


def crawl_row(context: Context, row: Dict[str, str]):
    entity = context.make("Person")
    name = row.pop("name")
    country = row.pop("country")
    entity.id = context.make_id(country, name)
    entity.add("name", name)
    entity.add("country", country)
    entity.add("notes", row.pop("notes"))
    context.emit(entity, target=True)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)
