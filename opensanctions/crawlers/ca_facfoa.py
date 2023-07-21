import csv
from pantomime.types import CSV

from opensanctions import helpers as h
from opensanctions.core import Context


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            proxy = context.make("Person")
            name = row.pop("name").strip()
            index = row.pop("index").strip()
            proxy.id = context.make_slug(index, name)
            proxy.add("name", name)
            proxy.add("alias", row.pop("aka", None))
            proxy.add("country", row.pop("country"))
            proxy.add("birthDate", row.pop("dob"))
            proxy.add("birthPlace", row.pop("pob"))
            proxy.add("notes", row.pop("notes"))
            proxy.add("topics", "sanction")

            sanction = h.make_sanction(context, proxy)
            sanction.add("program", row.pop("list"))
            sanction.add("sourceUrl", row.pop("source_url"))
            context.emit(sanction)
            context.emit(proxy, target=True)
            context.audit_data(row)
