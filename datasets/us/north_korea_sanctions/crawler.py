from typing import Dict
from rigour.mime.types import CSV
import csv
from datetime import datetime

from zavod import Context, helpers as h


def crawl_item(context: Context, row: Dict[str, str]):

    name = row.pop("Joint Venture Name")
    sector = row.pop("Sector")

    company = context.make("Company")
    company.id = context.make_id(name, sector)
    company.add("name", name)
    company.add("sector", sector)
    company.add("topics", "debarment")

    sanction = h.make_sanction(context, company)

    context.emit(company, target=True)
    context.emit(sanction)

    context.audit_data(row)


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_item(context, row)
