import csv
from pantomime.types import CSV
from typing import Dict

from zavod import Context
from zavod import helpers as h


def crawl_row(context: Context, row: Dict[str, str]):
    # https://archives.gov.ky/sites/legislativeassembly/portal/page/portal/lglhome/members/elected-2021-2025.html
    inception_date = "2021"
    dissolution_date = "2025"

    entity = context.make("Person")
    name = row.pop("Name")
    entity.id = context.make_id(name)
    entity.add("name", name)
    entity.add("title", row.pop("Title"))

    position = h.make_position(
        context,
        row.pop("Position"),
        topics=["gov.national"],
        country="ky",
    )
    occupancy = h.make_occupancy(
        context,
        entity,
        position,
        start_date=inception_date,
        end_date=dissolution_date,
    )
    context.emit(entity, target=True)
    context.emit(position)
    context.emit(occupancy)

def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)