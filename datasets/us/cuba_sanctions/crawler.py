import csv

from rigour.mime.types import CSV

from zavod import Context
from zavod import helpers as h
from zavod.shed.zyte_api import fetch_html

ORIGINAL_ACCOMMODATIONS_URL = (
    "https://www.state.gov/cuba-prohibited-accommodations-list-initial-publication/"
)
ACCOMMODATIONS_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQMquWjNWZ09dm9_mu9NKrxR33c6pe4hpiGFeheFT4tDZXwpelLudcYdCdME820aKJJo8TfMKbtoXTh/pub?gid=1890354374&single=true&output=csv"
ORIGINAL_RESTRICTED_ENTITIES_URL = "https://www.state.gov/division-for-counter-threat-finance-and-sanctions/cuba-restricted-list"
RESTRICTED_ENTITIES_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vQMquWjNWZ09dm9_mu9NKrxR33c6pe4hpiGFeheFT4tDZXwpelLudcYdCdME820aKJJo8TfMKbtoXTh/pub?gid=0&single=true&output=csv"
US_DOS_CU_REA = "US-DOS-CU-REA"
US_DOS_CU_PAL = "US-DOS-CU-PAL"
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
    # Chrome save HTML only
    # xmllint --format --html
    if not h.assert_dom_hash(node, "6dc9087e0ccb2e13fc2389ba4176ab114996ad32"):
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
            context.emit(proxy)
            sanction = h.make_sanction(context, proxy, program_key=US_DOS_CU_PAL)
            context.emit(sanction)
            context.audit_data(row, ignore=["City"])


def crawl_restricted_entities(context: Context):
    doc = fetch_html(
        context, ORIGINAL_RESTRICTED_ENTITIES_URL, CONTENT_XPATH, actions=ACTIONS
    )
    node = doc.find(CONTENT_XPATH)
    # Chrome save HTML only
    # xmllint --format --html
    if not h.assert_dom_hash(node, "2ebc4e59a53eb3064a2deb2b2a2828e178f9bdfe"):
        context.log.warning("Restricted List content changed. Check for data updates")

    path = context.fetch_resource("restricted_entities.csv", RESTRICTED_ENTITIES_URL)
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

            sanction = h.make_sanction(context, proxy, program_key=US_DOS_CU_REA)
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

            context.emit(proxy)
            context.emit(sanction)
            context.audit_data(row)


def crawl(context: Context):
    doc = fetch_html(context, context.dataset.url, CONTENT_XPATH, actions=ACTIONS)
    node = doc.find(CONTENT_XPATH)
    # Chrome save HTML only
    # xmllint --format --html
    if not h.assert_dom_hash(node, "0355144d3d290c3c617b2dd0077582a3136679f7"):
        context.log.warning("Landing page changed. Check for added/removed lists.")

    crawl_accommodations(context)
    crawl_restricted_entities(context)
