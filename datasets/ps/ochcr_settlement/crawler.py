import csv
from typing import Dict

from zavod import Context, helpers as h
from zavod.shed import zyte_api

# Program key for OHCHR Business and Human Rights database
OHCHR_BHR = "OHCHR-BHR"


def assert_database_hash(doc):
    tables_div = doc.xpath(
        "//div[@data-block-plugin-id='entity_browser_block:oh_accordion_component']"
    )
    assert len(tables_div) == 1, len(tables_div)
    # If the hash changes, update the 'table_xpath' below and review the page
    # structure for any new tables that may have been added.
    h.assert_dom_hash(tables_div[0], "b3fda7012ede1df28021b765c68d71f6e46755ca")


def crawl_row(context: Context, row: Dict[str, str]):
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
    # Only entities from the latest update are marked as 'debarred'
    entity.add("topics", row.pop("Topics"))
    entity.add("notes", row.pop("Notes").strip())
    entity.add("sourceUrl", row.pop("Response URL").strip())
    entity.add("sourceUrl", row.pop("Source URL").strip())

    sanction = h.make_sanction(context, entity, program_key=OHCHR_BHR)
    h.apply_date(sanction, "date", row.pop("Date"))

    context.emit(sanction)
    context.emit(entity)
    context.audit_data(row, ["ID"])


def crawl(context: Context):
    """Crawl the OHCHR database as converted to CSV"""
    # Check that no new tables have been added
    table_xpath = "//h5[contains(., 'Business enterprises involved in listed activities')]/following-sibling::div//table"
    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        table_xpath,
        html_source="httpResponseBody",
        absolute_links=True,
        cache_days=1,
    )
    assert_database_hash(doc)
    # Crawl the CSV version of the database
    path = context.fetch_resource("ohchr_database.csv", context.data_url)
    with open(path, "rt") as infh:
        reader = csv.DictReader(infh)
        for row in reader:
            crawl_row(context, row)
