import csv

from rigour.mime.types import CSV

from zavod import Context
from zavod import helpers as h
from zavod.shed.zyte_api import fetch_html

ORIGINAL_ACCOMMODATIONS_URL = (
    "https://www.state.gov/cuba-prohibited-accommodations-list-initial-publication/"
)
ACCOMMODATIONS_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQMquWjNWZ09dm9_mu9NKrxR33c6pe4hpiGFeheFT4tDZXwpelLudcYdCdME820aKJJo8TfMKbtoXTh/pub?gid=1890354374&single=true&output=csv"
# The Restricted Entities list is currently nowhere to be found, the last known public snapshot is at
# https://web.archive.org/web/20241209000143/https://www.state.gov/cuba-restricted-list/list-of-restricted-entities-and-subentities-associated-with-cuba-effective-january-8-2021/
# The resurrection of the list and addition of one entity were announced on 2025-01-31, see
# https://www.state.gov/restoring-a-tough-u-s-cuba-policy/. Our copy of the data, including the added entity, are
# available at our copy at ENTITIES_URL
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
    if not h.assert_dom_hash(node, "7a2be818841181e19a1988c4d86789087d245c31"):
        context.log.warning("Accommodations page changed. Check for data updates.")

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


def crawl_restricted_entities(context: Context):
    landing_doc = fetch_html(
        context, context.dataset.url, CONTENT_XPATH, actions=ACTIONS
    )
    # The restricted list URL page currently points to an interim page that refers to ORIGINAL_ACCOMMODATIONS_URL.
    # So the page doesn't really contain that list. We just want to alert when it changes
    restricted_list_url = landing_doc.xpath(".//a[text()='Cuba Restricted List']/@href")
    assert len(restricted_list_url) == 1, restricted_list_url
    content_xpath = ".//div[@id='content']"
    doc = fetch_html(context, restricted_list_url[0], content_xpath)
    node = doc.find(content_xpath)
    if not h.assert_dom_hash(node, "16aa93879cb1e04548ab319c749f9da1fa528a49"):
        context.log.warning(
            "Restricted List url destination content changed. Check for data updates"
        )

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
            proxy.add("sourceUrl", row.pop("SourceURL").split(";"))

            sanction = h.make_sanction(context, proxy)
            sanction.add("startDate", row.pop("EffectiveDate"))
            if h.is_active(sanction):
                proxy.add("topics", "sanction")

            parent = row.pop("Parent").strip()
            if len(parent):
                rel = context.make("Ownership")
                rel.id = context.make_id(parent, "owns", name)
                rel.add("owner", context.make_slug(parent))
                rel.add("asset", proxy.id)
                context.emit(rel)

            context.emit(proxy, target=True)
            context.emit(sanction)
            context.audit_data(row)


def crawl(context: Context):
    doc = fetch_html(context, context.dataset.url, CONTENT_XPATH, actions=ACTIONS)
    node = doc.find(CONTENT_XPATH)
    if not h.assert_dom_hash(node, "68b8e034bd290f5d384ab74bebf24f60b43db282"):
        context.log.warning("Landing page changed. Check for added/removed lists.")

    crawl_accommodations(context)
    crawl_restricted_entities(context)
