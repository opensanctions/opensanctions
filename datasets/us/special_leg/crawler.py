import csv
from pathlib import Path
from typing import Dict

from zavod import Context, helpers as h

LOCAL_PATH = Path(__file__).parent
FR_API_URL = "https://www.federalregister.gov/api/v1/documents.json?conditions[agencies][]=state-department&conditions[term]=nonproliferation+measures&order=newest"


def crawl_row(context: Context, row: Dict[str, str]):
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
    # any new notice triggers a warning, prompting a manual update of the
    # Google Sheet.
    h.assert_url_hash(context, FR_API_URL, "59f3eec13dbb3e2319784f767b8ca0b84bdecd16")
    rows, url = [], FR_API_URL
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
            if "Imposition of Nonproliferation Measures" in doc.get("title", "")
        )
        url = data.get("next_page_url")
    with open(LOCAL_PATH / "fr_notices.csv", "w") as f:
        writer = csv.writer(f)
        writer.writerow(["document_number", "publication_date", "html_url", "pdf_url"])
        writer.writerows(rows)


def crawl(context: Context):
    path = context.fetch_resource("source.csv", context.data_url)
    with open(path, "r") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            crawl_row(context, row)

    crawl_fr_notices(context)
