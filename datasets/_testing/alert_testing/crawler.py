import csv
from typing import Dict
from pantomime.types import CSV

from zavod import Context

# from zavod import helpers as h


def crawl_row(context: Context, row: Dict[str, str]):
    entity = context.make("Person")
    entity.id = context.make_slug(row.pop("id"))
    entity.add("name", row.pop("name"))
    entity.add("alias", row.pop("alias"))
    entity.add("birthDate", row.pop("dob"))
    entity.add("nationality", row.pop("nationality"))
    context.emit(entity, target=True)
    context.audit_data(row)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)
