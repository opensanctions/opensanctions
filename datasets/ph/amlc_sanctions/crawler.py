import csv
import re
import shutil
from pathlib import Path

from rigour.mime.types import CSV

from zavod import Context, helpers as h
from zavod.extract import zyte_api

SANCTIONS_CSV = Path(__file__).parent / "sanctions.csv"
RESOLUTIONS_CSV = Path(__file__).parent / "resolutions.csv"
# The cards are rendered client-side, well after the document has loaded, so the
# browser has to be told to wait for them rather than snapshotting the empty shell.
RESOLUTION_XPATH = ".//h3[contains(., 'TF-')]"
ACTIONS = [
    {
        "action": "waitForSelector",
        "selector": {"type": "xpath", "value": RESOLUTION_XPATH},
        "timeout": 15,
    },
]


def fetch_news_notices(context: Context) -> dict[str, str]:
    """Map each TF resolution id to the title of its notice in News and Announcements."""
    notices: dict[str, str] = {}
    page = 1
    while True:
        url = f"https://www.amlc.gov.ph/api/news/combined/feed?page={page}&limit=100"
        data = zyte_api.fetch_json(context, url, cache_days=1, geolocation="PH")
        assert data["success"] is True, (url, data)
        for item in data["data"]:
            title = " ".join(item["title"].split())
            # e.g. "Notice of Targeted Financial Sanctions through AMLC Resolution
            # No. TF-119, Series of 2026 in Relation to ..."
            match = re.search(r"AMLC Resolution No\. (TF-\d+)", title, re.IGNORECASE)
            if match is not None:
                notices[match.group(1).upper()] = title
        if page >= data["pagination"]["totalPages"]:
            return notices
        page += 1


def crawl_resolutions(context: Context) -> None:
    # sanctions.csv is a hand-maintained extraction of the designations in the
    # AMLC's terrorism financing resolutions, so a newly published resolution
    # has to reach a human rather than silently going missing from the dataset.
    # Rewriting resolutions.csv on every run turns one into a reviewable diff: check
    # the new resolution, add its designations to sanctions.csv, then commit both.
    assert context.dataset.url is not None
    # The validator doubles as the guarantee that the list actually rendered.
    doc = zyte_api.fetch_html(
        context,
        context.dataset.url,
        RESOLUTION_XPATH,
        actions=ACTIONS,
        cache_days=1,
        javascript=True,
        geolocation="PH",
    )
    published: dict[str, str] = {}
    for element in h.xpath_elements(doc, RESOLUTION_XPATH):
        # Drop the wrapping quotes; the heading then opens with its resolution id.
        title = h.element_text(element).strip('"')
        published[title.split(" ", 1)[0]] = title
    # Since TF-114, notices appear only under News and Announcements, not on the
    # resolutions page.
    for resolution_id, title in fetch_news_notices(context).items():
        published.setdefault(resolution_id, title)

    with open(RESOLUTIONS_CSV) as fh:
        known = {row["resolution_id"]: row["title"] for row in csv.DictReader(fh)}
    if published != known:
        context.log.warning(
            "Published AMLC resolutions changed. Reconcile sanctions.csv with the "
            "resolutions, then commit it with the updated resolutions.csv.",
            added=sorted(published.keys() - known.keys()),
            removed=sorted(known.keys() - published.keys()),
        )

    with open(RESOLUTIONS_CSV, "w") as fh:
        writer = csv.writer(fh)
        writer.writerow(["resolution_id", "title"])
        # The ids are zero-padded, so this is the newest-first order of the source.
        writer.writerows(sorted(published.items(), reverse=True))


def crawl_row(context: Context, row: dict[str, str]) -> None:
    name = row.pop("name")
    name_raw = row.pop("original_string")
    alias = row.pop("alias")
    resolution_no = row.pop("resolution_no")
    program = row.pop("program")

    entity = context.make("LegalEntity")
    entity.id = context.make_id(name, resolution_no)
    entity.add("name", name.split(";"), original_value=name_raw)
    entity.add("alias", alias.split(";") if alias else None, original_value=name_raw)
    entity.add("topics", "sanction")
    entity.add("country", "ph")
    entity.add("sourceUrl", row.pop("source_url"))
    entity.add("sourceUrl", row.pop("main_source_url"))

    sanction = h.make_sanction(
        context,
        entity,
        program_name=program,
        program_key=h.lookup_sanction_program_key(context, program),
    )
    sanction.add("program", resolution_no)
    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def crawl(context: Context) -> None:
    path = context.get_resource_path("source.csv")
    shutil.copy(SANCTIONS_CSV, path)
    context.export_resource(path, CSV, context.SOURCE_TITLE)
    with open(SANCTIONS_CSV, encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            crawl_row(context, row)
    crawl_resolutions(context)
