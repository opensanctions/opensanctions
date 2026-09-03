import csv
import shutil

from pathlib import Path
from typing import NamedTuple

from rigour.mime.types import CSV

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise, OccupancyStatus

# Hand-maintained, one row per appointment. See docs/extraction.md.
LOCAL_PATH = Path(__file__).parent
LEADERSHIP_FILE = LOCAL_PATH / "leadership.csv"


class Report(NamedTuple):
    """An edition: the `page` linking it (matched by check_updates) and the `pdf`
    holding the data (cited as sourceUrl)."""

    page: str
    pdf: str


# The editions `report` can cite. A new report trips check_updates().
REPORTS = {
    # Chairs, CEOs, board members, employee representatives and auditors.
    "2024": Report(
        page="https://www.regeringen.se/rapporter/2025/06/verksamhetsberattelse-for-bolag-med-statligt-agande-2024/",
        pdf="https://www.regeringen.se/contentassets/a2be3c80b3384f3eadc64530f6a2ff23/verksamhetsberattelse--for-bolag-med-statligt-agande-2024.pdf",
    ),
    # Chairs and CEOs only.
    "2025": Report(
        page="https://www.regeringen.se/rapporter/2026/07/arlig-information-om-bolag-med-statligt-agande-2025/",
        pdf="https://www.regeringen.se/contentassets/15cc81b6237b4aa49578a845b7e3dc94/arlig-information-om-bolag-med-statligt-agande-2025.pdf",
    ),
}
# Not read from the dataset's `url`, so editing metadata can't disarm check_updates().
SOURCE_PAGE_URL = (
    "https://www.regeringen.se/regeringens-politik/bolag-med-statligt-agande/"
)
# Both series titles: the report was renamed in 2026.
REPORTS_XPATH = (
    "//a[contains(text(), 'Verksamhetsberättelse för bolag med statligt ägande')"
    " or contains(text(), 'Årlig information om bolag med statligt ägande')]/@href"
)
# Not considered politically exposed.
SKIP_POSITIONS = ("Auditor", "Employee Representative")


def check_updates(context: Context) -> None:
    """Fail if the source page offers reports we haven't extracted.

    A new edition means: extract it per docs/extraction.md, add it to REPORTS, then
    merge its rows into leadership.csv.
    """
    doc = context.fetch_html(SOURCE_PAGE_URL, absolute_links=True)
    report_urls = set(h.xpath_strings(doc, REPORTS_XPATH))
    expected = {report.page for report in REPORTS.values()}
    assert report_urls == expected, report_urls.symmetric_difference(expected)


def check_source_urls(row: dict[str, str]) -> None:
    """Require source_url to be the source page plus one PDF per `report` year, so
    the two columns can't drift apart when edited by hand."""
    reports = h.multi_split(row["report"], ";")
    unknown = set(reports) - set(REPORTS)
    if unknown:
        raise ValueError(f"{LEADERSHIP_FILE.name}: unknown report(s) {unknown}")
    expected = {SOURCE_PAGE_URL} | {REPORTS[report].pdf for report in reports}
    found = set(h.multi_split(row["source_url"], ";"))
    if found != expected:
        raise ValueError(
            f"{LEADERSHIP_FILE.name}: source_url does not match report "
            f"{row['report']!r} for {row['name']!r}: "
            f"{found.symmetric_difference(expected)}"
        )


def crawl_row(context: Context, row: dict[str, str]) -> None:
    company_name = row.pop("company")
    pep_name = row.pop("name")
    position_name = row.pop("position")
    # Validated in crawl(); source_url is what we publish.
    row.pop("report")
    # Maintainer annotation, not the FollowTheMoney property. Not published.
    row.pop("notes")

    if position_name in SKIP_POSITIONS:
        return

    company = context.make("Company")
    company.id = context.make_id(company_name)
    company.add("name", company_name)
    company.add("previousName", context.lookup_value("previous_names", company_name))
    company.add("topics", "gov.soe")

    pep = context.make("Person")
    pep.id = context.make_id(pep_name)
    pep.add("name", pep_name)
    pep.add("alias", h.multi_split(row.pop("alias"), ";"))
    # 'country' rather than 'citizenship' as board members may be non-Swedish
    pep.add("country", "se")
    pep.add("sourceUrl", h.multi_split(row.pop("source_url"), ";"))

    position = h.make_position(
        context,
        name=f"{position_name}, {company_name}",
        topics=["gov.soe"],
        country="se",
        organization=company,
    )
    categorisation = categorise(context, position, default_is_pep=True)

    occupancy = h.make_occupancy(
        context,
        pep,
        position,
        False,
        categorisation=categorisation,
        status=OccupancyStatus.UNKNOWN,
    )

    if occupancy is not None:
        context.emit(pep)
        context.emit(company)
        context.emit(position)
        context.emit(occupancy)


def crawl(context: Context) -> None:
    check_updates(context)

    resource_path = context.get_resource_path(LEADERSHIP_FILE.name)
    shutil.copy(LEADERSHIP_FILE, resource_path)
    context.export_resource(resource_path, CSV, context.SOURCE_TITLE)

    with open(LEADERSHIP_FILE, encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))

    # A repeated key means an appointment was transcribed twice instead of gaining a
    # `report` year.
    seen: set[tuple[str, str, str]] = set()
    for row in rows:
        key = (row["name"], row["position"], row["company"])
        if key in seen:
            raise ValueError(f"{LEADERSHIP_FILE.name}: {key} appears twice")
        seen.add(key)
        check_source_urls(row)
        crawl_row(context, row)
