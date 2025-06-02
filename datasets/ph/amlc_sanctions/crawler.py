import csv
from typing import Dict

from zavod import Context, helpers as h


def crawl_row(context: Context, row: Dict[str, str]):
    name = row.pop("name")
    name_raw = row.pop("original_string")
    alias = row.pop("alias")
    resolution_no = row.pop("resolution_no")
    program = row.pop("program")

    entity = context.make("LegalEntity")
    entity.id = context.make_id(name, resolution_no)
    entity.add("name", name.split(";"), original_value=name_raw)
    entity.add("alias", alias.split(";") if alias else None, original_value=name_raw)
    entity.add("topics", "sanction")
    entity.add("country", "ph")
    entity.add("sourceUrl", row.pop("source_url"))
    entity.add("sourceUrl", row.pop("main_source_url"))
    context.emit(entity)

    sanction = h.make_sanction(
        context,
        entity,
        program_name=program,  # program_key=program
    )
    sanction.add("program", resolution_no)
    context.emit(sanction)

    context.audit_data(row)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    with open(path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            crawl_row(context, row)
