import csv
import shutil
from collections.abc import Iterator
from pathlib import Path

from normality import slugify
from rigour.mime.types import CSV

from zavod import Context
from zavod import helpers as h

# Both lists are published as web pages only, so the entity data is transcribed by
# hand into the CSV files next to this crawler and reviewed in git. WATCHED_PAGES
# guards that transcription: each hash covers the text of the page's list, so a
# revision upstream shows up as a warning. The runbook for reconciling one is in the
# dataset YAML. Hashes are text-only on purpose: the markup around these lists changes
# without the lists changing, and it makes a hash reproducible from any fetch of the
# page rather than only from a rendered one.
#
# state.gov answers a bare `curl/*` user agent with a "Technical Difficulties" page, but
# serves the real page to zavod's own user agent, so these pages need no unblocking.
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
PAL_PROGRAM = "US-DOS-CU-PAL"
REA_PROGRAM = "US-DOS-CU-REA"
# One-shot migration for the IDs retired by moving alias names out of `Name`, and by
# splitting the two Camagüey Plaza rows onto the hotel they both describe. Remove these
# once the dataset has run in production, per zavod/docs/best_practices/entity_id.md.
REKEYED = [
    (
        "blau-marina-varadero-resort-aka-fiesta-americana-punta-varadero-fiesta-club-adults-only",
        "blau-marina-varadero-resort",
    ),
    ("hotel-kawama-aka-club-kawama", "hotel-kawama"),
    (
        "iberostar-bella-vista-aka-iberostar-selection-bella-vista-varadero",
        "iberostar-bella-vista",
    ),
    ("playa-larga-aka-horizontes-playa-larga", "playa-larga"),
    ("villa-guama-aka-horizontes-villa-guama", "villa-guama"),
    ("melia-cayo-santa-maria-aka-sol-cayo-santa-maria", "melia-cayo-santa-maria"),
    ("villa-la-granjita-aka-horizontes-la-granjita", "villa-la-granjita"),
    ("villa-los-caneyes-aka-horizontes-los-caneyes", "villa-los-caneyes"),
    (
        "warwick-cayo-santa-maria-aka-labranda-cayo-santa-maria-hotel",
        "warwick-cayo-santa-maria",
    ),
    ("ma-dolores-aka-horizontes-finca-ma-dolores", "ma-dolores"),
    ("pestana-cayo-coco-aka-hotel-playa-paraiso", "pestana-cayo-coco"),
    ("marea-del-portillo-aka-club-amigo-marea-del-portillo", "marea-del-portillo"),
    (
        "blau-costa-verde-beach-resort-aka-fiesta-americana-holguin-costa-verde",
        "blau-costa-verde-beach-resort",
    ),
    ("villa-don-lino-also-hotel-don-lino", "villa-don-lino"),
    (
        "club-amigo-carisol-los-corales-aka-carisol-los-corales",
        "club-amigo-carisol-los-corales",
    ),
    ("san-basilio-aka-hotel-e-san-basilio", "san-basilio"),
    ("plaza-also-hotel-islazul-plaza-camaguey", "hotel-plaza-camaguey"),
    ("also-fiesta-americana-punta-varadero", "blau-marina-varadero-resort"),
    ("also-fiesta-club-adults-only", "blau-marina-varadero-resort"),
    ("also-labranda-cayo-santa-maria-hotel", "warwick-cayo-santa-maria"),
    ("also-fiesta-americana-holguin-costa-verde", "blau-costa-verde-beach-resort"),
    ("also-hotel-playa-paraiso", "pestana-cayo-coco"),
    (
        "alias-empresa-de-certificacion-de-sistemas-de-seguridad-y-proteccion",
        "agencia-de-certificacion-y-consultoria-de-seguridad-y-proteccion",
    ),
    ("alias-ais-remesas", "american-international-services"),
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


def row_id(context: Context, row: dict[str, str]) -> str:
    """Resolve a row's `Id` column to an entity ID.

    The column exists so that correcting a name never changes an entity's identity.
    That only holds while the value is exactly the slug the ID is built from, so an
    edit that would silently produce a different ID is rejected rather than emitted.
    """
    value = row.pop("Id").strip()
    if not len(value):
        raise ValueError("Row is missing an Id")
    if value != slugify(value, sep="-"):
        raise ValueError(f"Id is not a slug: {value!r}")
    entity_id = context.make_slug(value)
    if entity_id is None:
        raise ValueError(f"Id does not yield an entity ID: {value!r}")
    return entity_id


def check_watched_pages(context: Context) -> None:
    """Warn when a watched page's list no longer matches the reviewed CSV files.

    The lists are only fetched to be compared, never to be parsed, so a fetch failure
    must not stop the dataset from publishing data that is already in the repository.
    """
    for url, expected, message in WATCHED_PAGES:
        try:
            matched = h.assert_html_url_hash(
                context, url, expected, path=CONTENT_XPATH, text_only=True
            )
        except Exception as exc:
            context.log.warning("Could not fetch watched page", url=url, error=str(exc))
            continue
        if not matched:
            context.log.warning(message, url=url)


def crawl_accommodations(context: Context) -> None:
    for row in source_rows(context, "accommodations.csv"):
        proxy = context.make("Company")
        proxy.id = row_id(context, row)
        proxy.add("name", row.pop("Name").strip())
        proxy.add("alias", row.pop("Alias").split(";"))
        proxy.add("country", "Cuba")
        proxy.add("address", row.pop("Address"))
        proxy.add("sourceUrl", row.pop("SourceURL"))
        proxy.add("topics", "sanction")
        context.emit(proxy)
        sanction = h.make_sanction(
            context, proxy, key=PAL_PROGRAM, program_key=PAL_PROGRAM
        )
        context.emit(sanction)
        context.audit_data(row, ignore=["City"])


def crawl_restricted_entities(context: Context) -> None:
    rows = list(source_rows(context, "restricted_entities.csv"))
    known_ids = {row["Id"].strip() for row in rows}
    for row in rows:
        proxy = context.make("Company")
        parent = row.pop("Parent").strip()
        proxy.id = row_id(context, row)
        proxy.add("name", row.pop("Company").strip())
        proxy.add("country", "Cuba")
        proxy.add("alias", row.pop("Acronym"))
        proxy.add("alias", row.pop("Alias").split(";"))
        proxy.add("sector", row.pop("Sector"))
        proxy.add("classification", row.pop("Category"))
        proxy.add("sourceUrl", row.pop("SourceURL").split(";"))

        sanction = h.make_sanction(
            context, proxy, key=REA_PROGRAM, program_key=REA_PROGRAM
        )
        sanction.add("startDate", row.pop("EffectiveDate"))
        if h.is_active(sanction):
            proxy.add("topics", "sanction")

        if len(parent):
            if parent not in known_ids:
                raise ValueError(f"Parent is not a listed entity: {parent!r}")
            rel = context.make("Ownership")
            rel.id = context.make_id(parent, "owns", proxy.id)
            rel.add("owner", context.make_slug(parent))
            rel.add("asset", proxy.id)
            context.emit(rel)

        context.emit(proxy)
        context.emit(sanction)
        context.audit_data(row)


def crawl(context: Context) -> None:
    for old_id, new_id in REKEYED:
        context.rekey(context.make_slug(old_id), context.make_slug(new_id))
    crawl_accommodations(context)
    crawl_restricted_entities(context)
    check_watched_pages(context)
