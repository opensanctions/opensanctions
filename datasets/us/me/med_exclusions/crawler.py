import json
import re
from datetime import datetime
from typing import Any
from urllib.parse import quote, urljoin

from rigour.mime.types import PDF
from zavod.extract import zyte_api

from zavod import Context, settings
from zavod import helpers as h


def crawl_item(row: dict[str, str | None], context: Context) -> None:
    if first_name := row.pop("provider_first_name"):
        last_name = row.pop("provider_last_name")
        middle_initial = row.pop("provider_mi")
        position = row.pop("provider_type")

        entity = context.make("Person")
        entity.id = context.make_id(first_name, last_name, middle_initial)
        h.apply_name(
            entity,
            first_name=first_name,
            last_name=last_name,
            middle_name=middle_initial,
        )
        if position and position.lower() != "other":
            entity.add("position", position)
    else:
        last_name = row.pop("provider_last_name")
        entity = context.make("Company")
        entity.id = context.make_id(last_name)
        entity.add("name", last_name)
        entity.add("sector", row.pop("provider_type"))

    for i in [1, 2, 3, 4]:
        alias_first_name = row.pop(f"alias_first_name_{i}")
        alias_last_name = row.pop(f"alias_last_name_{i}")

        if alias_first_name is None and alias_last_name is None:
            continue

        # The source lists people in the alias columns of a company, but
        # gives no role for them.
        if entity.schema.name == "Company":
            person = context.make("Person")
            person.id = context.make_id(alias_first_name, alias_last_name)
            h.apply_name(person, first_name=alias_first_name, last_name=alias_last_name)
            person.add("country", "us")

            link = context.make("UnknownLink")
            link.id = context.make_id(person.id, entity.id)
            link.add("object", entity)
            link.add("subject", person)

            context.emit(link)
            context.emit(person)

        else:
            h.apply_name(
                entity,
                first_name=alias_first_name,
                last_name=alias_last_name,
                alias=True,
            )

    entity.add("country", "us")
    entity.add("topics", "debarment")

    sanction = h.make_sanction(context, entity)

    h.apply_date(sanction, "startDate", row.pop("state_exclusion_start_date"))

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def fetch_grid_items(context: Context, url: str) -> list[dict[str, Any]]:
    """Return the document library items embedded in the Kendo grid script."""
    script_xpath = "//div[@id='gridSPDLI']/following-sibling::script[1]"
    doc = zyte_api.fetch_html(
        context,
        url,
        script_xpath,
        html_source="httpResponseBody",
        geolocation="us",
    )
    script = h.element_text(h.xpath_element(doc, script_xpath), squash=False)
    match = re.search(r'"data":(\{"Data":\[.*?\],"Total":\d+\})', script)
    assert match is not None, f"Grid data not found: {url}"
    data = json.loads(match.group(1))
    items: list[dict[str, Any]] = data["Data"]
    assert len(items) == data["Total"], (url, len(items), data["Total"])
    return items


def clean_cell(value: str | None) -> str | None:
    # A wrapped line ends in a space when the break falls between words, and
    # in a letter or hyphen when the report splits a long word.
    if value is None:
        return None
    text = value.replace(" \n", " ").replace("\n", "").strip()
    return text if len(text) > 0 else None


def crawl(context: Context) -> None:
    # The library holds one folder per month, in no particular order.
    folders: dict[datetime, str] = {}
    for item in fetch_grid_items(context, context.data_url):
        if item["Type"] != "folder":
            raise ValueError(f"Unexpected item in report library: {item}")
        month = datetime.strptime(item["Name"], "%B %Y")
        folders[month] = item["ServerRelativeUrl"]
    # folders maps each month to its path relative to data_url, e.g.
    # datetime(2026, 10, 1) -> "Monthly Provider Exclusion Report--October 2026".
    # max() picks the latest month, which holds the current report.
    latest_month = max(folders)
    months_behind = (settings.RUN_TIME.year - latest_month.year) * 12 + (
        settings.RUN_TIME.month - latest_month.month
    )
    if months_behind > 3:
        context.log.warning(
            "Latest monthly report is more than 3 months old, check if the "
            "source is still updated",
            latest_month=latest_month.strftime("%B %Y"),
            url=context.data_url,
        )
    folder_url = urljoin(context.data_url, quote(folders[latest_month]))

    files = fetch_grid_items(context, folder_url)
    assert len(files) == 1, f"Expected one report in {folder_url}: {files}"
    (report,) = files
    assert report["Type"] == ".pdf", report
    report_name: str = report["Name"]
    assert "/" not in report_name, report_name
    pdf_url = f"{folder_url}/{quote(report_name)}"

    _, _, _, path = zyte_api.fetch_resource(
        context, report_name, pdf_url, expected_media_type=PDF, geolocation="us"
    )
    context.export_resource(path, PDF, title=context.SOURCE_TITLE)
    for row in h.parse_pdf_table(
        context,
        path,
        headers_per_page=True,
        # Keeps the trailing space that clean_cell relies on.
        page_settings=lambda page: (page, {"text_keep_blank_chars": True}),
    ):
        crawl_item({key: clean_cell(value) for key, value in row.items()}, context)
