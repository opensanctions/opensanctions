import csv
from rigour.mime.types import CSV
from normality import collapse_spaces
from typing import Dict

from zavod import Context, helpers as h


def crawl_row(context: Context, row: Dict[str, str]):
    entity_type = row.pop("Sanction Type")
    schema = context.lookup_value("schema", entity_type)
    if schema is None:
        context.log.warning(f"Unknown schema: {entity_type}")
        return

    name = row.pop("Name")
    position = row.pop("Position").strip()

    entity = context.make(schema)
    entity.id = context.make_id(name, position)

    entity.add("name", name)
    entity.add("country", row.pop("Country"))
    entity.add("topics", "sanction.counter")
    if schema == "Person" and position.replace("-", ""):
        entity.add("position", position)

    sanction = h.make_sanction(context, entity)
    sanction_date = collapse_spaces(row.pop("Sanction Date"))
    h.apply_date(sanction, "date", sanction_date)
    sanction.add("description", row.pop("Sanction Title"))
    sanction.add("sourceUrl", row.pop("Source URL"))

    context.emit(entity)
    context.emit(sanction)
    context.audit_data(row, ignore=["Number"])


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)
