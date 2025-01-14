import csv
from rigour.mime.types import CSV

from zavod import Context
from zavod import helpers as h
from zavod.shed.zyte_api import fetch_html

ORIGINAL_ACCOMMODATIONS_URL = "https://www.state.gov/cuba-sanctions/cuba-prohibited-accommodations-list/cuba-prohibited-accommodations-list-initial-publication/"
ACCOMMODATIONS_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQMquWjNWZ09dm9_mu9NKrxR33c6pe4hpiGFeheFT4tDZXwpelLudcYdCdME820aKJJo8TfMKbtoXTh/pub?gid=1890354374&single=true&output=csv"
ORIGINAL_ENTITIES_URL = "https://www.state.gov/cuba-restricted-list/list-of-restricted-entities-and-subentities-associated-with-cuba-effective-january-8-2021/"
ENTITIES_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQMquWjNWZ09dm9_mu9NKrxR33c6pe4hpiGFeheFT4tDZXwpelLudcYdCdME820aKJJo8TfMKbtoXTh/pub?gid=0&single=true&output=csv"
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


def crawl_accommodations(context: Context):
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
            context.emit(proxy)
            context.audit_data(row, ignore=["City"])


def crawl_restricted_entities(context: Context):
    doc = fetch_html(context, ORIGINAL_ENTITIES_URL, CONTENT_XPATH, actions=ACTIONS)
    node = doc.find(CONTENT_XPATH)
    h.assert_dom_hash(node, "d51568c7e6acb7da68cbf6c2a54987ea6fd5ff53")

    path = context.fetch_resource("restricted_entities.csv", ENTITIES_URL)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        for row in csv.DictReader(fh):
            proxy = context.make("Company")
            name = row.pop("Company").strip()
            proxy.id = context.make_slug(name)
            proxy.add("name", name)
            proxy.add("country", "Cuba")
            proxy.add("alias", row.pop("Acronym"))
            proxy.add("sector", row.pop("Sector"))
            proxy.add("classification", row.pop("Category"))
            proxy.add("sourceUrl", row.pop("SourceURL"))
            proxy.add("topics", "sanction")

            sanction = h.make_sanction(context, proxy)
            sanction.add("startDate", row.pop("EffectiveDate"))

            parent = row.pop("Parent").strip()
            if len(parent):
                rel = context.make("Ownership")
                rel.id = context.make_id(parent, "owns", name)
                rel.add("owner", context.make_slug(parent))
                rel.add("asset", proxy.id)
                context.emit(rel)

            context.emit(proxy)
            context.emit(sanction)
            context.audit_data(row)


def crawl(context: Context):
    crawl_accommodations(context)
    crawl_restricted_entities(context)
