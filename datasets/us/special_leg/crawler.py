import csv
import shutil
from pathlib import Path
from typing import Any, cast
from urllib.parse import urljoin, urlparse

import requests
from lxml import html
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
    "_fields": "date,link,title",
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


def compare_url(url: str) -> str:
    """Normalise a URL for comparison against a source_url in the CSV files.

    The published posts link their PDFs over http while the CSV records them
    over https, and a trailing slash is not meaningful here.
    """
    parsed = urlparse(url.strip())
    return parsed._replace(scheme="", path=parsed.path.rstrip("/")).geturl()


def parse_pdf_links(content: str, base_url: str) -> list[str]:
    """Find the list documents linked from a published post."""
    root = html.fromstring(content)
    urls: dict[str, None] = {}
    for anchor in h.xpath_elements(root, "//a[@href]"):
        url = urljoin(base_url, cast(str, anchor.get("href")))
        if urlparse(url).path.lower().endswith(".pdf"):
            urls[url] = None
    return list(urls)


def check_section_1286_lists(context: Context) -> None:
    """Warn about published Section 1286 lists that are not in the CSV yet.

    A post counts as reviewed once one of the documents it links is recorded as
    the source_url of a Section 1286 row, so importing a new fiscal-year list
    mutes its warning. A post that is not a list at all — the search is a plain
    keyword query — is muted through the reviewed_urls config instead. Note that
    this detects new and re-issued documents, not an edit to the bytes behind a
    URL that is already imported.
    """
    source_urls = set()
    with open(SECTION_1286_FILE, encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            for url in h.multi_split(row["source_url"], ";"):
                source_urls.add(compare_url(url))

    discovery = context.dataset.config.get("discovery", {})
    reviewed_urls = {
        compare_url(str(url)) for url in discovery.get("reviewed_urls", [])
    }

    posts = context.fetch_json(
        CTO_API_URL, params=CTO_API_PARAMS, cache_days=CTO_CACHE_DAYS
    )
    if not isinstance(posts, list):
        raise ValueError(f"Unexpected response from {CTO_API_URL}")
    if len(posts) == 0:
        # The known lists are all still published, so an empty result means the
        # site, the API or the search behaviour has changed.
        context.log.warning("Section 1286 list search returned no posts")
        return

    for post in posts:
        link = cast(str, post["link"])
        if compare_url(link) in reviewed_urls:
            continue
        content = context.fetch_text(link, cache_days=CTO_CACHE_DAYS) or ""
        pdf_urls = parse_pdf_links(content, link)
        if any(compare_url(url) in source_urls for url in pdf_urls):
            continue
        context.log.warning(
            "Unreviewed Section 1286 list",
            title=cast(dict[str, Any], post["title"])["rendered"],
            date=post["date"],
            url=link,
            documents=pdf_urls,
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
    except (requests.RequestException, ValueError) as exc:
        context.log.warning("Section 1286 list discovery failed", error=str(exc))
