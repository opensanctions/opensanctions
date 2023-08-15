from pathlib import Path
from typing import Dict
import csv
from datetime import datetime

from zavod.context import Context
from zavod import helpers as h

LOCAL_PATH = Path(__file__).parent / "dataset.csv"


def crawl_row(context: Context, row: Dict[str, str]):
    person = context.make("Person")
    name = row.pop("name")
    person.id = context.make_slug(name)
    person.add("name", name)
    context.emit(person, target=True)

    position = h.make_position(
        context, name=row.pop("position"), country=row.pop("country").split(",")
    )
    start_date = row.pop("start_date") or None
    end_date = row.pop("end_date") or None
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        row.pop("no_end_means_current") == "true",
        datetime(2023, 8, 8),
        start_date,
        end_date,
    )
    context.emit(position)
    context.emit(occupancy)


def crawl(context: Context):
    with open(LOCAL_PATH, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)
