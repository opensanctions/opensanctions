import csv
import shutil
from datetime import date
from pathlib import Path

from rigour.mime.types import CSV

from zavod.extract import zyte_api

from zavod import Context, settings
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


def crawl_company(
    context: Context, rows: list[dict[str, str]], debarment_from: date
) -> None:
    """Emit a company and one Sanction spanning every edition that lists it.

    The Sanction's startDate is the date the company was first included on the
    list, i.e. the publication of the edition or Federal Register notice that
    added it, so it equals the listingDate. Section 1260H attaches no consequence
    to the listing itself: those come from other statutes, each with its own
    date. The procurement ban that makes a listing a debarment applies from
    `debarment_from`, set in the dataset config, and gates the topic instead of
    the startDate.
    """
    rows.sort(key=lambda row: row["Start date"])
    latest = rows[-1]
    entity = context.make("Company")
    clean_name = latest["Clean Name"]
    entity.id = context.make_id(clean_name)
    sanction = h.make_sanction(context, entity, program_key=PROGRAM_KEY)
    h.apply_date(sanction, "listingDate", rows[0]["Start date"])
    # Each edition is the complete list, so the latest row decides whether the
    # company is still listed.
    end_date = latest["End date"]
    h.apply_date(sanction, "endDate", end_date)
    h.apply_date(sanction, "modifiedAt", end_date)

    previous: tuple[str, ...] | None = None
    for row in rows:
        start_date = row.pop("Start date")
        row.pop("End date")
        # The printed name changes with the PDF's formatting, so only the parts
        # of the entry the list states count as an amendment.
        entry = (row["Alias"], row["Previous Name"], row["Parent Name"], row["Note"])
        if previous is not None and entry != previous:
            h.apply_date(sanction, "modifiedAt", start_date)
        previous = entry

        row.pop("Clean Name")
        raw_name = row.pop("Name")
        alias = row.pop("Alias")
        entity.add("name", clean_name, original_value=raw_name)
        alias_prop = "weakAlias" if len(alias) <= 5 and alias.isupper() else "alias"
        entity.add(alias_prop, alias, original_value=raw_name)
        entity.add("previousName", h.multi_split(row.pop("Previous Name"), [";"]))

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

        sanction.add("reason", row.pop("Note"))
        sanction.add("summary", row.pop("Removal Note"))
        sanction.add("sourceUrl", row.pop("Source Url"))
        sanction.add("sourceUrl", row.pop("Removal Url"))
        context.audit_data(row)

    h.finalize_sanction(
        context, sanction, effective_on_listing=True, amendments_dated=True
    )
    if h.is_active(sanction):
        debarred = settings.RUN_TIME.date() >= debarment_from
        entity.add("topics", "debarment" if debarred else "reg.warn")
    elif not sanction.has("endDate"):
        # Transcribed ahead of the date the edition or notice takes effect.
        entity.add("topics", "reg.warn")

    context.emit(sanction)
    context.emit(entity)


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

    debarment_from = date.fromisoformat(context.dataset.config["debarment_from"])
    companies: dict[str, list[dict[str, str]]] = {}
    with open(source_file, encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            companies.setdefault(row["Clean Name"], []).append(row)
    for rows in companies.values():
        crawl_company(context, rows, debarment_from)

    check_releases(context)
