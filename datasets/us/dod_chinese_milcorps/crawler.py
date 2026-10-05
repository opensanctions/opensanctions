import csv
import shutil
from pathlib import Path

from rigour.mime.types import CSV

from zavod.extract import zyte_api

from zavod import Context
from zavod import helpers as h

LOCAL_PATH = Path(__file__).parent
PROGRAM_KEY = "US-DOD-1260H"
RELEASES_URL = "https://www.defense.gov/News/Releases/Search/1260H/"
RELEASES_XPATH = ".//div[@class='alist stories release-list']"
# The release list as last reviewed:
#
# June 30, 2026  Department of War Launches New Website to Help Industry Partners
#                Navigate Section 805 Supply Chain Requirements
# June 8, 2026   DOD Releases List of Chinese Military Companies in Accordance with
#                Section 1260H of the NDAA for Fiscal Year 2021
# Jan. 8, 2026   The War Department Strengthens Measures to Protect DOW-Funded Research
# Jan. 7, 2025   DOD Releases List of Chinese Military Companies ...
# Jan. 31, 2024  DOD Releases List of People's Republic of China (PRC) Military Companies ...
# Oct. 5, 2022   DOD Releases List of People's Republic of China (PRC) Military Companies ...
# June 3, 2021   DOD Releases List of Chinese Military Companies ...
RELEASES_HASH = "646c3ba9b4d41eeac34d828e9694ca4f50d88481"


def crawl_row(context: Context, row: dict[str, str]) -> None:
    """Emit one listing of one company."""
    entity = context.make("Company")
    clean_name = row.pop("Clean Name")
    raw_name = row.pop("Name")
    alias = row.pop("Alias")
    entity.id = context.make_id(clean_name)
    entity.add("name", clean_name, original_value=raw_name)
    alias_prop = "weakAlias" if len(alias) <= 5 and alias.isupper() else "alias"
    entity.add(alias_prop, alias, original_value=raw_name)
    entity.add("previousName", row.pop("Previous Name"))
    entity.add("sourceUrl", row.pop("Source Url"))
    entity.add("notes", row.pop("Note"))
    entity.add("topics", "debarment")

    parent_name = row.pop("Parent Name")
    if parent_name and parent_name != clean_name:
        parent = context.make("Company")
        parent.id = context.make_id(parent_name)
        parent.add("name", parent_name)
        context.emit(parent)

        own = context.make("Ownership")
        own.id = context.make_id("ownership", clean_name, parent_name)
        own.add("owner", parent)
        own.add("asset", entity)
        context.emit(own)

    start_date = row.pop("Start date")
    # Sanction per listing period: startDate = date added, endDate = date removed.
    # start_date also keys the sanction to avoid collisions when entities are re-listed.
    sanction = h.make_sanction(
        context,
        entity,
        key=start_date,
        program_key=PROGRAM_KEY,
        start_date=start_date,
        end_date=row.pop("End date"),
    )
    context.emit(sanction)
    context.emit(entity)
    context.audit_data(row)


def check_releases(context: Context) -> None:
    """Warn when DoD publishes a release that may announce a list update.

    Each edition of the Section 1260H list is announced as a press release
    linking a PDF, which is transcribed into sanctions.csv by hand.
    """
    doc = zyte_api.fetch_html(context, RELEASES_URL, RELEASES_XPATH, cache_days=1)
    search_result = h.xpath_element(doc, RELEASES_XPATH)
    h.assert_dom_hash(search_result, RELEASES_HASH, text_only=True)


def crawl(context: Context) -> None:
    source_file = LOCAL_PATH / "sanctions.csv"
    resource_path = context.get_resource_path("source.csv")
    shutil.copy(source_file, resource_path)
    context.export_resource(resource_path, CSV, context.SOURCE_TITLE)

    with open(source_file, encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)

    check_releases(context)
