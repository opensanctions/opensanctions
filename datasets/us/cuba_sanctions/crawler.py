import csv
import shutil
from collections.abc import Iterator
from pathlib import Path

from rigour.mime.types import CSV
from zavod.extract.zyte_api import fetch_html

from zavod import Context, settings
from zavod import helpers as h

# Both lists are published as web pages only, so the entity data is transcribed by
# hand into the CSV files next to this crawler and reviewed in git. WATCHED_PAGES
# guards that transcription: each hash covers the text of the page's list, so a
# revision upstream shows up as a warning. The runbook for reconciling one is in the
# dataset YAML. Hashes are text-only on purpose -- the markup around these lists
# changes without the lists changing, and a rendered DOM does not serialise
# identically to a raw HTTP response.
LOCAL_PATH = Path(__file__).parent
ACCOMMODATIONS_URL = (
    "https://www.state.gov/cuba-sanctions/cuba-prohibited-accommodations-list"
)
RESTRICTED_ENTITIES_URL = "https://www.state.gov/division-for-counter-threat-finance-and-sanctions/cuba-restricted-list"
WATCHED_PAGES = [
    (
        "https://www.state.gov/cuba-sanctions/",
        "1c0b7848505ace0d1871710834faf885d59ef6b6",
        "Landing page changed. Check for added/removed lists.",
    ),
    (
        ACCOMMODATIONS_URL,
        "0e37bcfcc69cf7381a48ad28d881722c69e41415",
        "Accommodations list changed. Reconcile accommodations.csv.",
    ),
    (
        RESTRICTED_ENTITIES_URL,
        "2d5cd6154e8bdd793d7ddaff2c48ba3253edfb36",
        "Restricted list changed. Reconcile restricted_entities.csv.",
    ),
]
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


def check_watched_pages(context: Context) -> None:
    """Warn when a watched page's list no longer matches the reviewed CSV files.

    The lists are only fetched to be compared, never to be parsed, so a fetch failure
    must not stop the dataset from publishing data that is already in the repository.
    """
    if settings.ZYTE_API_KEY is None:
        context.log.info("Skipping page change detection: no Zyte API key configured")
        return
    for url, expected, message in WATCHED_PAGES:
        try:
            doc = fetch_html(context, url, CONTENT_XPATH, actions=ACTIONS)
        except Exception as exc:
            context.log.warning("Could not fetch watched page", url=url, error=str(exc))
            continue
        node = doc.find(CONTENT_XPATH)
        if not h.assert_dom_hash(node, expected, text_only=True):
            context.log.warning(message, url=url)


def crawl_accommodations(context: Context) -> None:
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
    crawl_accommodations(context)
    crawl_restricted_entities(context)
    check_watched_pages(context)
