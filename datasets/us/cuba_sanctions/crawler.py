import csv
import shutil
from collections.abc import Iterator
from pathlib import Path

from rigour.mime.types import CSV
from zavod.extract.zyte_api import fetch_html

from zavod import Context
from zavod import helpers as h

# Both lists are published as web pages only, so the entity data is transcribed by
# hand into the CSV files next to this crawler and reviewed in git. The hashes below
# watch the pages so a change upstream surfaces as a warning; the runbook for
# reconciling one is in the dataset YAML.
LOCAL_PATH = Path(__file__).parent
ORIGINAL_ACCOMMODATIONS_URL = (
    "https://www.state.gov/cuba-prohibited-accommodations-list-initial-publication/"
)
ORIGINAL_RESTRICTED_ENTITIES_URL = "https://www.state.gov/division-for-counter-threat-finance-and-sanctions/cuba-restricted-list"
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


def source_rows(context: Context, name: str) -> Iterator[dict[str, str]]:
    """Yield the rows of one of the hand-maintained list files in this directory.

    Also copies the file into the run's resources, so the exported dataset ships the
    exact input it was built from.
    """
    source_file = LOCAL_PATH / name
    resource_path = context.get_resource_path(name)
    shutil.copy(source_file, resource_path)
    context.export_resource(resource_path, CSV, title=context.SOURCE_TITLE)
    with open(source_file, encoding="utf-8", newline="") as fh:
        yield from csv.DictReader(fh)


def crawl_accommodations(context: Context) -> None:
    doc = fetch_html(
        context, ORIGINAL_ACCOMMODATIONS_URL, CONTENT_XPATH, actions=ACTIONS
    )
    node = doc.find(CONTENT_XPATH)
    if not h.assert_dom_hash(node, "6dc9087e0ccb2e13fc2389ba4176ab114996ad32"):
        context.log.warning("Accommodations page changed. Check for data updates.")

    for row in source_rows(context, "accommodations.csv"):
        proxy = context.make("Company")
        name = row.pop("Name").strip()
        proxy.id = context.make_slug(name)
        proxy.add("name", name)
        proxy.add("country", "Cuba")
        proxy.add("address", row.pop("Address"))
        proxy.add("sourceUrl", row.pop("SourceURL"))
        proxy.add("topics", "sanction")
        context.emit(proxy)
        sanction = h.make_sanction(context, proxy, program_key="US-DOS-CU-PAL")
        context.emit(sanction)
        context.audit_data(row, ignore=["City"])


def crawl_restricted_entities(context: Context) -> None:
    doc = fetch_html(
        context, ORIGINAL_RESTRICTED_ENTITIES_URL, CONTENT_XPATH, actions=ACTIONS
    )
    node = doc.find(CONTENT_XPATH)
    if not h.assert_dom_hash(node, "a146ff14f0a283a4a80afaaf0f46637574aa78c2"):
        context.log.warning("Restricted List content changed. Check for data updates")

    for row in source_rows(context, "restricted_entities.csv"):
        proxy = context.make("Company")
        name = row.pop("Company").strip()
        proxy.id = context.make_slug(name)
        proxy.add("name", name)
        proxy.add("country", "Cuba")
        proxy.add("alias", row.pop("Acronym"))
        proxy.add("sector", row.pop("Sector"))
        proxy.add("classification", row.pop("Category"))
        proxy.add("sourceUrl", row.pop("SourceURL").split(";"))

        sanction = h.make_sanction(context, proxy, program_key="US-DOS-CU-REA")
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


def crawl(context: Context) -> None:
    assert context.dataset.url is not None
    doc = fetch_html(context, context.dataset.url, CONTENT_XPATH, actions=ACTIONS)
    node = doc.find(CONTENT_XPATH)
    if not h.assert_dom_hash(node, "0355144d3d290c3c617b2dd0077582a3136679f7"):
        context.log.warning("Landing page changed. Check for added/removed lists.")

    crawl_accommodations(context)
    crawl_restricted_entities(context)
