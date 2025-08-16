import csv
from rigour.mime.types import CSV

from zavod import Context


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            giin = row.pop("GIIN")
            name = row.pop("FINm")
            country = row.pop("CountryNm")
            entity = context.make("Company")
            entity.id = context.make_id(giin, name, country)
            entity.add("name", name)
            entity.add("country", country)
            entity.add("giiNumber", giin)
            entity.add("topics", "fin")
            context.emit(entity)
