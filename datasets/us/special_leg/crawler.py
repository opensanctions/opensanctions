import csv
import shutil
from pathlib import Path
from typing import cast

import requests
from rigour.mime.types import CSV

from zavod import Context, helpers as h

LOCAL_PATH = Path(__file__).parent
# One CSV per act, report or designation authority covered by the dataset.
DATA_PATH = LOCAL_PATH / "data"
SECTION_1286_FILE = DATA_PATH / "section_1286.csv"
FR_API_URL = "https://www.federalregister.gov/api/v1/documents.json?conditions[agencies][]=state-department&conditions[term]=nonproliferation+measures&order=newest"
# The Section 1286 lists are published by the DoD Chief Technology Officer as a
# WordPress post per fiscal year, each linking that year's list as a PDF. The
# post slug carries the fiscal year, so a new edition is a new post rather than
# a change to an existing one.
CTO_API_URL = "https://www.cto.mil/wp-json/wp/v2/posts"
CTO_API_PARAMS = {
    "search": "Section 1286",
    "per_page": "20",
    "_fields": "date,link",
}
CTO_CACHE_DAYS = 1


def crawl_row(context: Context, row: dict[str, str]) -> None:
    """Process one row of the CSV data"""
    schema = row.pop("schema")
    name = row.pop("name")
    report_date = row.pop("report-date")
    topics = row.pop("topics")
    source_url = row.pop("source_url")
    program = row.pop("program")
    entity = context.make(schema)
    entity.id = context.make_slug(name)
    entity.add("topics", topics)
    h.apply_name(entity, name)
    entity.add("alias", row.pop("aliases").split(";"))
    entity.add("country", row.pop("country"))
    entity.add("sourceUrl", source_url.strip())
    entity.add("notes", row.pop("notes"))
    sanction = h.make_sanction(
        context,
        entity,
        program_name=program,
        program_key=h.lookup_sanction_program_key(context, program),
    )
    h.apply_date(sanction, "listingDate", report_date)
    h.apply_date(sanction, "startDate", row.pop("start-date"))
    h.apply_date(sanction, "endDate", row.pop("end-date"))
    sanction.add("reason", row.pop("reason"))
    sanction.add("description", f"Published in {report_date} report.")
    sanction.set("authority", row.pop("authority"))
    sanction.set("sourceUrl", h.multi_split(source_url, ";"))

    context.emit(entity)
    context.emit(sanction)
    context.audit_data(row)


def crawl_fr_notices(context: Context) -> None:
    # The ISN component of the US Trade Consolidated Screening List (CSL) is
    # fed from the State Department's nonproliferation sanctions page, which is
    # updated manually and with no guaranteed cadence. In practice, new INKSNA
    # designations published in the Federal Register can take weeks or months
    # to appear in the CSL. This function monitors the FR API directly so that
    # any new notice triggers a warning.
    # If the hash changes, review the updated fr_notices.csv for new entries and
    # add the designations to data/inksna.csv accordingly. Then commit the updated
    # fr_notices.csv and update the hash in this function.
    h.assert_url_hash(context, FR_API_URL, "9ee76295f4ac089fe7382bf6f33b947dae5f9eb0")
    rows: list[list[str]] = []
    url = FR_API_URL
    while url:
        data = context.fetch_json(url)
        rows.extend(
            [
                doc["document_number"],
                doc["publication_date"],
                doc["html_url"],
                doc["pdf_url"],
            ]
            for doc in data.get("results", [])
            if "Imposition of Nonproliferation Measures" in doc.get("title")
        )
        url = data.get("next_page_url")

    with open(LOCAL_PATH / "fr_notices.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(["document_number", "publication_date", "html_url", "pdf_url"])
        writer.writerows(rows)


def without_scheme(url: str) -> str:
    """Strip what differs between a document as linked and as recorded: the
    posts link their PDFs over http, the CSV records them over https."""
    return url.strip().split("://", 1)[-1]


def list_documents(context: Context, url: str) -> list[str]:
    """Find the list documents linked from a published post."""
    doc = context.fetch_html(url, cache_days=CTO_CACHE_DAYS, absolute_links=True)
    links = h.xpath_elements(doc, "//a[@href]")
    urls = [cast(str, link.get("href")) for link in links]
    return [url for url in urls if url.lower().endswith(".pdf")]


def check_section_1286_lists(context: Context) -> None:
    """Warn about published Section 1286 lists that are not in the CSV yet.

    A post is reviewed once one of the documents it links is the source_url of a
    Section 1286 row, so importing a new fiscal-year list mutes its own warning.
    """
    with open(SECTION_1286_FILE, encoding="utf-8", newline="") as fh:
        imported = {
            without_scheme(url)
            for row in csv.DictReader(fh)
            for url in h.multi_split(row["source_url"], ";")
        }

    posts = context.fetch_json(
        CTO_API_URL, params=CTO_API_PARAMS, cache_days=CTO_CACHE_DAYS
    )
    # Every known list is still published, so an empty result means the site,
    # the API or the search behaviour has changed.
    if len(posts) == 0:
        context.log.warning("Section 1286 list search returned no posts")

    for post in posts:
        documents = list_documents(context, post["link"])
        if any(without_scheme(url) in imported for url in documents):
            continue
        context.log.warning(
            "Unreviewed Section 1286 list",
            url=post["link"],
            date=post["date"],
            documents=documents,
        )


def crawl_source_file(context: Context, source_file: Path) -> None:
    """Emit the entities of one act, report or designation authority."""
    resource_path = context.get_resource_path(source_file.name)
    shutil.copy(source_file, resource_path)
    context.export_resource(
        resource_path, CSV, f"{context.SOURCE_TITLE}: {source_file.name}"
    )

    with open(source_file, encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))

    # Each file covers exactly one legal basis, so that a new act is added as a
    # new file rather than mixed into an existing one.
    programs = {row["program"] for row in rows}
    if len(programs) != 1:
        raise ValueError(f"{source_file.name} covers {len(programs)} programs")

    # An entity is listed at most once per edition of a list: repeated names
    # within an edition mean the source was transcribed twice, or with one row
    # per alias instead of one row per entity.
    seen: set[tuple[str, str]] = set()
    for row in rows:
        listing = (row["report-date"], row["name"])
        if listing in seen:
            raise ValueError(
                f"{source_file.name}: {row['name']!r} listed twice "
                f"in the {row['report-date']!r} edition"
            )
        seen.add(listing)
        crawl_row(context, row)


def crawl(context: Context) -> None:
    source_files = sorted(DATA_PATH.glob("*.csv"))
    if len(source_files) == 0:
        raise ValueError(f"No source data found in {DATA_PATH}")
    for source_file in source_files:
        crawl_source_file(context, source_file)

    crawl_fr_notices(context)

    try:
        check_section_1286_lists(context)
    except requests.RequestException as exc:
        context.log.warning("Section 1286 list discovery failed", error=str(exc))
