import csv
from typing import Dict

from zavod import Context, helpers as h


def crawl_row(context: Context, row: Dict[str, str]):
    full_name = row.pop("name")
    offense = row.pop("offense")
    case_number = row.pop("case number")

    entity = context.make("Person")
    entity.id = context.make_id(full_name, case_number, offense)
    entity.add("name", full_name)
    entity.add("alias", row.pop("alias", "").split(";"))
    entity.add("position", row.pop("position", "").split(";"))
    entity.add("topics", "wanted")
    entity.add("country", "ph")
    entity.add("notes", case_number)
    entity.add("sourceUrl", row.pop("source"))

    sanction = h.make_sanction(context, entity, program_name=row.pop("list"))
    sanction.add("reason", offense)

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row, ignore=["jor-no", "reward"])


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    with open(path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            crawl_row(context, row)
