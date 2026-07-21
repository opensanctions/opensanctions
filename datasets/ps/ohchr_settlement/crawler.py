import csv
from pathlib import Path
from typing import Dict

from zavod.extract import zyte_api

from zavod import Context
from zavod import helpers as h

PROGRAM_KEY = "OHCHR-BHR"


def crawl_row(context: Context, row: Dict[str, str]) -> None:
    name = row.pop("Business enterprise").strip()
    activities = row.pop("Sub-paragraph of listed activity (2020 report)").strip()
    country = row.pop("State concerned").strip()
    section = row.pop("Section").strip()
    # Skip entities that are no longer involved
    if section.startswith("A. ") or "no longer involved" in section:
        return

    entity = context.make("Company")
    entity.id = context.make_id(name, country)
    h.apply_name(entity, name)
    entity.add("previousName", row.pop("Previous name").strip())
    entity.add("program", section)
    entity.add("notes", f"Listed activities: {activities}")
    entity.add("topics", "debarment")
    entity.add("notes", row.pop("Notes").strip())
    entity.add("sourceUrl", row.pop("Response URL").strip())
    entity.add("sourceUrl", row.pop("Source URL").strip())

    sanction = h.make_sanction(context, entity, program_key=PROGRAM_KEY)
    h.apply_date(sanction, "date", row.pop("Date"))

    context.emit(sanction)
    context.emit(entity)
    context.audit_data(row, ["ID"])


def crawl(context: Context) -> None:
    """Crawl the OHCHR database as converted to CSV"""
    # Check that no new tables have been added
    content_xpath = "//div[contains(@class, 'ohchr-layout__container')]"
    assert context.dataset.url is not None
    doc = zyte_api.fetch_html(context, context.dataset.url, content_xpath, cache_days=1)
    content = h.xpath_elements(doc, content_xpath, expect_exactly=1)
    # OHCHR only publishes the database as HTML tables on the page, so the actual data is a
    # hand-curated Google Sheet (the dataset's data_url) mirroring it. The page has two tables,
    # written to table.csv one after the other (the second table's "No." restarts at 1): the
    # current listing, followed by a smaller table of enterprises that have been delisted. To
    # sync the Sheet when the OHCHR page changes:
    #   1. Run the crawler locally. It rewrites table.csv from the OHCHR page and logs a
    #      "DOM hash changed" warning with the new hash; diff table.csv to see what changed.
    #   2. Add newly listed enterprises from the first table to the Google Sheet. For each
    #      enterprise in the delisted table, remove it from the Sheet if present (do not add
    #      it). These edits live in Google Drive, not the PR.
    #   3. Put the new hash below, then commit crawler.py + table.csv and re-run to confirm.
    rows = [
        h.cells_to_str(row)
        for table in h.xpath_elements(content[0], ".//table")
        for row in h.parse_html_table(table)
    ]
    with open(Path(__file__).parent / "table.csv", "w") as fh:
        writer = csv.DictWriter(fh, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    # Check if the data has been updated, normally with a new report, if the content has changed.
    h.assert_dom_hash(
        content[0], "66e1e163a93d72deb0ad257d2069004f70afba0f", text_only=True
    )
    # 2018: OHCHR published an interim report (A/HRC/37/39), setting out the methods of work and methodology used to discharge the mandate as set out in Council resolution 31/36.
    # 2020: OHCHR published a report (A/HRC/43/71) containing a database listing 112 business enterprises.
    # 2023: OHCHR published an update containing a review of the 112 business enterprises.
    # 2024: OHCHR published a report (A/HRC/57/21) detailing the legal and human rights foundations for the work, as well as an updated methodology for updating the database. The report also contains preliminary data from an initial screening of the submissions received in response to a call for input made in May 2024.
    # 2025: OHCHR published a report (A/HRC/60/19) containing an update of the database listing 158 business enterprises.

    # Crawl the CSV version of the database
    path = context.fetch_resource("ohchr_database.csv", context.data_url)
    with open(path, "rt") as infh:
        reader = csv.DictReader(infh)
        for row in reader:
            crawl_row(context, row)
