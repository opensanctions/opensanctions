import csv
import shutil
from pathlib import Path

from rigour.mime.types import CSV

from zavod import Context, helpers as h

LOCAL_PATH = Path(__file__).parent
# One CSV per act, report or designation authority covered by the dataset.
DATA_PATH = LOCAL_PATH / "data"
FR_API_URL = "https://www.federalregister.gov/api/v1/documents.json?conditions[agencies][]=state-department&conditions[term]=nonproliferation+measures&order=newest"


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
