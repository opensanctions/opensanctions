import csv
from rigour.mime.types import CSV

from zavod import Context


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            name_raw = row.pop("name_raw")
            name_en = row.pop("name_en")
            address = row.pop("address")
            entity = context.make("LegalEntity")
            entity.id = context.make_id(name_raw, name_en)
            entity.add("address", address)
            entity.add("name", name_en)
            entity.add("alias", name_raw)
            entity.add("sourceUrl", row.pop("source_url"))
            entity.add("topics", "debarment")
            context.emit(entity)
