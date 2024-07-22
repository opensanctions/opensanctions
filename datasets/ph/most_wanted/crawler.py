import csv
from typing import Dict
import zavod.helpers as h
from zavod import Context


def crawl_row(context: Context, row: Dict[str, str]):
    data = dict(row)
    full_name = data.pop("name", None)
    offense = data.pop("offense", None)
    case_number = data.pop("case number", None)
    source = data.pop("source", None)
    reward = data.pop("reward", None)

    entity = context.make("Person")
    entity.id = context.make_id(full_name, case_number, reward)
    entity.add("name", full_name)
    entity.add("sourceUrl", source)
    entity.add("topics", "wanted")

    sanction = h.make_sanction(context, entity)
    sanction.add("reason", offense)
    sanction.add("program", "Most Wanted")
    # Emit the entities
    context.emit(entity, target=True)
    context.emit(sanction)
    # Log warnings if there are unhandled fields remaining in the dict
    context.audit_data(data)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    with open(path, "r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            crawl_row(context, row)
