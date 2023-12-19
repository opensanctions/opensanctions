import csv
from typing import Dict
from pantomime.types import CSV

from zavod import Context


def crawl_row(context: Context, row: Dict[str, str]):
    entity = context.make("Company")
    name = row.pop("name")
    entity.id = context.make_slug(name)
    entity.add("name", name)
    entity.add("leiCode", row.pop("lei"))
    entity.add("permId", row.pop("perm_id"))

    security = context.make("Security")
    isin = row.pop("isin")
    security.id = f"isin-{isin}"
    security.add("isin", isin)
    security.add("issuer", entity)

    context.emit(entity)
    context.emit(security)
    context.audit_data(row)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)
