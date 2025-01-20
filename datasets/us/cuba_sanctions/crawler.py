import csv
from rigour.mime.types import CSV

from zavod import Context
from zavod import helpers as h
from zavod.shed.zyte_api import fetch_html

ORIGINAL_ACCOMMODATIONS_URL = "https://www.state.gov/cuba-sanctions/cuba-prohibited-accommodations-list/cuba-prohibited-accommodations-list-initial-publication/"
ACCOMMODATIONS_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQMquWjNWZ09dm9_mu9NKrxR33c6pe4hpiGFeheFT4tDZXwpelLudcYdCdME820aKJJo8TfMKbtoXTh/pub?gid=1890354374&single=true&output=csv"
CONTENT_XPATH = ".//div[@class='entry-content']"
ACTIONS = [
    {
        "action": "waitForSelector",
        "selector": {
            "type": "xpath",
            "value": CONTENT_XPATH,
        },
        "timeout": 15,
    },
]


def crawl(context: Context):
    doc = fetch_html(
        context, ORIGINAL_ACCOMMODATIONS_URL, CONTENT_XPATH, actions=ACTIONS
    )
    node = doc.find(CONTENT_XPATH)
    h.assert_dom_hash(node, "7a2be818841181e19a1988c4d86789087d245c31")

    path = context.fetch_resource("accommodations.csv", ACCOMMODATIONS_URL)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            proxy = context.make("Company")
            name = row.pop("Name").strip()
            proxy.id = context.make_slug(name)
            proxy.add("name", name)
            proxy.add("country", "Cuba")
            proxy.add("address", row.pop("Address"))
            proxy.add("sourceUrl", row.pop("SourceURL"))
            proxy.add("topics", "sanction")
            context.emit(proxy, target=True)
            context.audit_data(row, ignore=["City"])
