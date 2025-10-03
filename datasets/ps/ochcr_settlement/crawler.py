import csv
from typing import Dict

from zavod.shed import zyte_api

from zavod import Context
from zavod import helpers as h

PROGRAM_KEY = "OHCHR-BHR"


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
    entity.add("topics", "debarment")
    entity.add("notes", row.pop("Notes").strip())
    entity.add("sourceUrl", row.pop("Response URL").strip())
    entity.add("sourceUrl", row.pop("Source URL").strip())

    sanction = h.make_sanction(context, entity, program_key=PROGRAM_KEY)
    h.apply_date(sanction, "date", row.pop("Date"))

    context.emit(sanction)
    context.emit(entity)
    context.audit_data(row, ["ID"])


def crawl(context: Context):
    """Crawl the OHCHR database as converted to CSV"""
    # Check that no new tables have been added
    content_xpath = "//div[contains(@class, 'ohchr-layout__container')]"
    doc = zyte_api.fetch_html(context, context.dataset.url, content_xpath, cache_days=1)
    content = doc.xpath(content_xpath)
    assert len(content) == 1, len
    # Check if the data has been updated, normally with a new report, if the content has changed.
    h.assert_dom_hash(
        content[0], "565a7ae05da5fcc0527c7a7f75ec832fb05b6fbe", text_only=True
    )

    # Crawl the CSV version of the database
    path = context.fetch_resource("ohchr_database.csv", context.data_url)
    with open(path, "rt") as infh:
        reader = csv.DictReader(infh)
        for row in reader:
            crawl_row(context, row)
