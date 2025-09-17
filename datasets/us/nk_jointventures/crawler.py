from typing import Dict
from rigour.mime.types import CSV, PDF
import csv

from zavod import Context, helpers as h
from zavod.shed.zyte_api import fetch_html, fetch_resource


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
    doc = fetch_html(
        context, context.dataset.url, table_xpath, cache_days=1, absolute_links=True
    )
    table = doc.xpath(table_xpath)
    assert len(table) == 1, "Expected exactly one table in the document"
    link = table[0].xpath(".//a/@href")
    assert len(link) == 1, "Expected exactly one link in the table"

    # Expect
    # North Korea Sanctions & Enforcement Actions Advisory | 827.97 KB | 08/03/2018
    # Assert hash of linked PDF (hopefully less fickle than HTML)
    _, _, _, pdf_path = fetch_resource(context, "source.pdf", link[0], PDF)
    h.assert_file_hash(pdf_path, "cd9894479b1330bf0db3885ded3254e580af7acd")

    csv_path = context.fetch_resource("source.csv", context.data_url)
    context.export_resource(csv_path, CSV, title=context.SOURCE_TITLE)
    with open(csv_path, "r") as fh:
        for row in csv.DictReader(fh):
            crawl_item(context, row)
