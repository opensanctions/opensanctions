import csv
from rigour.mime.types import CSV

from zavod import Context
from zavod import helpers as h


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            proxy = context.make("Person")
            name = row.pop("name").strip()
            index = row.pop("index").strip()
            program = row.pop("list").strip()
            proxy.id = context.make_slug(index, name)
            proxy.add("name", name)
            proxy.add("alias", row.pop("aka", None))
            proxy.add("country", row.pop("country"))
            proxy.add("birthDate", row.pop("dob"))
            proxy.add("birthPlace", row.pop("pob"))
            proxy.add("notes", row.pop("notes"))

            sanction = h.make_sanction(
                context,
                proxy,
                program_name=program,
                source_program_key=program,
                program_key=h.lookup_sanction_program_key(context, program),
            )
            sanction.add("sourceUrl", row.pop("source_url"))

            sanction.add("startDate", row.pop("start_date", None))
            sanction.add("endDate", row.pop("end_date", None))

            if sanction.has("endDate"):
                continue

            proxy.add("topics", "sanction")
            context.emit(sanction)
            context.emit(proxy)
            context.audit_data(row)
