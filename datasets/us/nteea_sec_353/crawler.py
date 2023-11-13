from pantomime.types import CSV
from typing import Dict
import csv

from zavod import Context
from zavod import helpers as h


def crawl_row(context: Context, row: Dict[str, str]):
    entity = context.make("Person")
    name = row.pop("name")
    country = row.pop("country")
    entity.id = context.make_id(country, name)
    entity.add("name", name)
    entity.add("country", country)
    entity.add("topics", "sanction")

    sanction = h.make_sanction(context, entity)
    report_date = row.pop("report-date")
    sanction.add("listingDate", report_date)
    sanction.add("reason", row.pop("reason"))
    sanction.add("program","Section 353(b) of the United States - Northern Triangle Enhanced Engagement Act")
    sanction.add("description", f"Published in {report_date} report.")

    context.emit(entity, target=True)
    context.emit(sanction)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)
