from typing import Dict
from rigour.mime.types import CSV
import csv

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_html


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
    table_xpath = ".//table[@aria-label='Table of Files associated with page']"
    doc = fetch_html(context, context.dataset.url, table_xpath, cache_days=1)
    doc.make_links_absolute(context.dataset.url)
    table = doc.xpath(table_xpath)
    assert len(table) == 1, "Expected exactly one table in the document"
    link = table[0].xpath(".//a/@href")
    assert len(link) == 1, "Expected exactly one link in the table"

    # Expect
    # North Korea Sanctions & Enforcement Actions Advisory | 827.97 KB | 08/03/2018
    # Assert hash of linked PDF (hopefully less fickle than HTML)
    h.assert_url_hash(context, link[0], "cd9894479b1330bf0db3885ded3254e580af7acd")

    path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_item(context, row)
