import csv
import shutil
from pathlib import Path
from typing import Any

from rigour.mime.types import CSV

from zavod import Context, helpers as h

LOCAL_PATH = Path(__file__).parent
ECFR_VERSIONS_URL = (
    "https://www.ecfr.gov/api/versioner/v1/versions/title-15.json"
    "?part=744&section=744.22"
)


def crawl_row(context: Context, row: dict[str, str]) -> None:
    entity = context.make(row.pop("Type"))
    name = row.pop("Name")
    country = row.pop("Country")
    entity.id = context.make_id(country, name)
    entity.add("name", name, lang="eng")
    entity.add("alias", row.pop("Alias").split(";"), lang="eng")
    entity.add("country", country)
    entity.add("topics", row.pop("Topics").split(";"))
    sanction = h.make_sanction(context, entity, program_key=row.pop("Program"))
    h.apply_date(sanction, "startDate", row.pop("Date"))
    h.apply_date(sanction, "endDate", row.pop("End date"))
    sanction.add("sourceUrl", row.pop("Source URL"))
    context.emit(sanction)
    context.emit(entity)
    context.audit_data(row)


def check_section_versions(context: Context) -> None:
    """Warn when 15 CFR 744.22 has amendments not yet reflected in mieu.csv.

    The named end users exist only in the regulation text, so there is no list
    document to poll. Instead, the eCFR versioner API reports one version per
    amendment to the section; any amendment date missing from the reviewed set
    in the dataset config needs a human to diff paragraph (f)(2) and update the
    CSV (see the runbook comment in the yml).
    """
    discovery: dict[str, Any] = context.dataset.config.get("discovery", {})
    reviewed = {str(date) for date in discovery.get("reviewed_versions", [])}
    payload = context.fetch_json(ECFR_VERSIONS_URL, cache_days=2)
    if not isinstance(payload, dict) or "content_versions" not in payload:
        raise ValueError("eCFR versions response missing 'content_versions'")
    versions = payload["content_versions"]
    if not isinstance(versions, list) or len(versions) == 0:
        raise ValueError("eCFR versions response contains no versions")

    latest = versions[-1]
    latest_name = str(latest.get("name") or "")
    if latest.get("removed") is True or "military-intelligence" not in latest_name:
        context.log.warning(
            "15 CFR 744.22 may have been removed or redesignated",
            name=latest_name,
            date=latest.get("amendment_date"),
        )

    for version in versions:
        date = str(version.get("amendment_date"))
        if date in reviewed:
            continue
        context.log.warning(
            "Unreviewed amendment to 15 CFR 744.22",
            date=date,
            section_url=f"https://www.ecfr.gov/on/{date}/title-15/section-744.22",
            text_api_url=(
                f"https://www.ecfr.gov/api/versioner/v1/full/{date}/title-15.xml"
                "?part=744&section=744.22"
            ),
        )


def crawl(context: Context) -> None:
    source_file = LOCAL_PATH / "mieu.csv"
    resource_path = context.get_resource_path("source.csv")
    shutil.copy(source_file, resource_path)
    context.export_resource(resource_path, CSV, context.SOURCE_TITLE)

    with open(source_file, encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            crawl_row(context, row)

    try:
        check_section_versions(context)
    except Exception as exc:
        context.log.warning("eCFR section version check failed", error=str(exc))
