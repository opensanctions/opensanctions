import csv
from typing import Dict
from zavod import Context


def crawl_row(context: Context, row: Dict[str, str]):
    full_name = row.get("name")
    offense = row.get("offense")
    case_number = row.get("case number")

    entity = context.make("Person")
    entity.id = context.make_id(full_name, case_number, offense)
    entity.add("name", full_name)
    entity.add("topics", "wanted")
    entity.add("country", "ph")
    entity.add("notes", offense)
    entity.add("notes", case_number)
    # Emit the entities
    context.emit(entity)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    with open(path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            crawl_row(context, row)
