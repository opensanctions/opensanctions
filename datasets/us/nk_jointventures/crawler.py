from typing import Dict
from rigour.mime.types import CSV
import csv

from zavod import Context, helpers as h


def crawl_item(context: Context, row: Dict[str, str]):

    name = row.pop("Joint Venture Name")
    sector = row.pop("Sector")

    company = context.make("Company")
    company.id = context.make_id(name, sector)
    company.add("name", name)
    company.add("sector", sector)
    company.add("topics", "export.risk")
    company.add("country", "North Korea")

    context.emit(company)

    context.audit_data(row)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.dataset.url, cache_days=1)
    table = doc.xpath(".//table[@aria-label='Table of Files associated with page']")
    assert len(table) == 1, "Expected exactly one table in the document"
    h.assert_dom_hash(table[0], "86bc75bd5bd6ba998c7533323122f766e4007dfa")
    # North Korea Sanctions & Enforcement Actions Advisory
    # Last Modified: Aug 06, 2018

    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_item(context, row)
