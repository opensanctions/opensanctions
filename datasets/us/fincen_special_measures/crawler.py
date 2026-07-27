import csv
import re
import shutil
from collections.abc import Generator
from pathlib import Path
from typing import cast

from lxml import html
from normality import collapse_spaces, slugify, squash_spaces
from rigour.mime.types import CSV
from zavod.entity import Entity

from zavod import Context
from zavod import helpers as h

LOCAL_PATH = Path(__file__).parent
REGEX_DATE = re.compile(r"(\d{1,2}/\d{1,2}/\d{4})")
RELATIONSHIPS = {"self", "target", "subsidiary", "owner", "related"}


def convert_date(date_str: str) -> list[str]:
    """Convert various date formats to 'YYYY-MM-DD'."""
    return REGEX_DATE.findall(date_str)


def load_details(context: Context) -> dict[str, list[dict[str, str]]]:
    """Read details.csv, grouping the mined rows by their measure join key.

    The CSV is the frozen record of facts mined from the rulemaking documents
    linked in the special measures table (see README.md). The live crawl only
    parses the table; everything document-derived comes from this file.
    """
    source_file = LOCAL_PATH / "details.csv"
    resource_path = context.get_resource_path("details.csv")
    shutil.copy(source_file, resource_path)
    context.export_resource(resource_path, CSV, "Mined rulemaking document details")

    details: dict[str, list[dict[str, str]]] = {}
    with open(source_file, encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            measure = squash_spaces(row.pop("Measure"))
            details.setdefault(measure, []).append(row)
    return details


def crawl_detail(
    context: Context,
    detail: dict[str, str],
    main: Entity,
    measure_names: set[str],
    listing_date: str,
    start_date: str,
    end_date: str,
) -> None:
    relationship = detail.pop("Relationship")
    if relationship not in RELATIONSHIPS:
        raise ValueError(f"Unknown relationship: {relationship!r}")
    name = detail.pop("Name")
    source_url = detail.pop("Source URL")
    if source_url == "":
        raise ValueError(f"Detail row without Source URL: {name!r}")

    if relationship == "self":
        main.add("alias", h.multi_split(detail.pop("Alias"), [";"]))
        main.add("country", detail.pop("Country"))
        main.add("address", detail.pop("Address"))
        main.add("registrationNumber", detail.pop("Registration number"))
        main.add("notes", detail.pop("Notes"))
        detail.pop("Type")
        detail.pop("Topics")
        context.audit_data(detail)
        return

    entity = context.make(detail.pop("Type"))
    if squash_spaces(name) in measure_names:
        # The document links two measures' main entities (e.g. Huione Group
        # owning H-Pay Service PLC): attach to the existing entity instead of
        # minting a duplicate. Mirrors the main entity's ID construction.
        entity.id = context.make_id(name)
    else:
        entity.id = context.make_id(main.id, name)
    entity.add("name", name)
    entity.add("alias", h.multi_split(detail.pop("Alias"), [";"]))
    entity.add("country", detail.pop("Country"))
    entity.add("address", detail.pop("Address"))
    entity.add("notes", detail.pop("Notes"))
    entity.add("sourceUrl", source_url)
    reg_number = detail.pop("Registration number")
    if entity.schema.is_a("LegalEntity"):
        entity.add("registrationNumber", reg_number)
    elif reg_number != "":
        raise ValueError(f"Registration number on non-LegalEntity: {name!r}")
    entity.add("topics", h.multi_split(detail.pop("Topics"), [";"]))

    if relationship == "target":
        sanction = h.make_sanction(context, entity)
        sanction.add("sourceUrl", source_url)
        for date in convert_date(listing_date):
            h.apply_date(sanction, "listingDate", date)
        for date in convert_date(start_date):
            h.apply_date(sanction, "startDate", date)
        for date in convert_date(end_date):
            h.apply_date(sanction, "endDate", date)
        context.emit(sanction)
    elif relationship in ("subsidiary", "owner"):
        owner, asset = (
            (main, entity) if relationship == "subsidiary" else (entity, main)
        )
        ownership = context.make("Ownership")
        ownership.id = context.make_id("ownership", owner.id, asset.id)
        ownership.add("owner", owner.id)
        ownership.add("asset", asset.id)
        ownership.add("sourceUrl", source_url)
        context.emit(ownership)
    elif relationship == "related":
        link = context.make("UnknownLink")
        link.id = context.make_id("link", main.id, entity.id)
        link.add("subject", main.id)
        link.add("object", entity.id)
        link.add("sourceUrl", source_url)
        context.emit(link)

    context.emit(entity)
    context.audit_data(detail)


def crawl_item(
    context: Context,
    row: dict[str, html.HtmlElement],
    details: list[dict[str, str]],
    measure_names: set[str],
) -> list[str]:
    """Emit the measure's main entity and its mined details; return the
    document URLs linked from the table row."""
    name = row.pop("name").text_content()
    schema = context.lookup_value("target_type", name)
    if schema is None:
        schema = "Company"
    entity = context.make(schema)
    entity.id = context.make_id(name)
    entity.add("name", name)
    # Adjust the topic based on the presence of "final rule"
    final_rule = collapse_spaces(row["final-rule"].text_content().strip().lower())
    rescinded_date = squash_spaces(row["rescinded"].text_content())
    if (
        final_rule
        and final_rule != "---"
        and (not rescinded_date or rescinded_date == "---")
    ):
        entity.add("topics", "sanction")
    else:
        entity.add("topics", "reg.warn")

    # Create and add details to the sanction
    sanction = h.make_sanction(context, entity)

    # Extract document links
    anchors = row["finding"].findall(".//a")
    anchors.extend(row["notice-of-proposed-rulemaking"].findall(".//a"))
    anchors.extend(row["rescinded"].findall(".//a"))
    anchors.extend(row["final-rule"].findall(".//a"))
    doc_urls = [url for a in anchors if (url := a.get("href")) is not None]
    sanction.add("sourceUrl", doc_urls)

    finding_date = row["finding"].text_content()
    nprm_date = row["notice-of-proposed-rulemaking"].text_content()
    listing_date = finding_date if finding_date else nprm_date
    for date in convert_date(listing_date):
        h.apply_date(sanction, "listingDate", date)

    final_rule_date = row["final-rule"].text_content()
    start_date = final_rule_date if final_rule_date != "---" else ""
    for date in convert_date(start_date):
        h.apply_date(sanction, "startDate", date)

    end_date = rescinded_date if rescinded_date not in ("---", "") else ""
    for date in convert_date(end_date):
        h.apply_date(sanction, "endDate", date)

    for detail in details:
        crawl_detail(
            context, detail, entity, measure_names, listing_date, start_date, end_date
        )

    context.emit(entity)
    context.emit(sanction)
    return doc_urls


# Parse the table and yield rows as dictionaries.
def parse_table(
    table: html.HtmlElement,
) -> Generator[dict[str, html.HtmlElement], None, None]:
    headers: list[str] | None = None
    for row in table.findall(".//tr"):
        if headers is None:
            slugged: list[str | None] = []
            for el in row.findall("./th"):
                # Workaround because lxml-stubs doesn't yet support HtmlElement
                # https://github.com/lxml/lxml-stubs/pull/71
                eltree = cast(html.HtmlElement, el)
                slugged.append(slugify(eltree.text_content()))
            assert slugged[0] is None, slugged
            # no duplicate column headers
            assert len(set(slugged)) == len(slugged), slugged
            assert all(hdr is not None for hdr in slugged[1:]), slugged
            headers = ["name"] + [cast(str, hdr) for hdr in slugged[1:]]
            continue

        cells = row.findall("./td")
        if len(cells) == 1:
            continue
        assert len(headers) == len(cells), (headers, cells)
        yield {hdr: c for hdr, c in zip(headers, cells)}


# Main crawl function to fetch and process data.
def crawl(context: Context) -> None:
    details = load_details(context)
    reviewed_urls = {
        str(url)
        for url in context.dataset.config.get("discovery", {}).get("reviewed_urls", [])
    }
    for detail_rows in details.values():
        reviewed_urls.update(d["Source URL"] for d in detail_rows if d["Source URL"])

    doc = context.fetch_html(context.data_url, absolute_links=True)
    table = h.xpath_element(doc, '//table[@id="special-measures-table"]')
    rows = list(parse_table(table))
    measure_names = {squash_spaces(row["name"].text_content()) for row in rows}
    unreviewed: dict[str, list[str]] = {}
    for row in rows:
        measure = squash_spaces(row["name"].text_content())
        doc_urls = crawl_item(context, row, details.pop(measure, []), measure_names)
        for url in doc_urls:
            if url not in reviewed_urls:
                unreviewed.setdefault(url, []).append(measure)

    if len(details) > 0:
        # A mined measure no longer matches any table row: FinCEN renamed or
        # removed it, and the join key in details.csv needs to follow.
        raise ValueError(f"details.csv measures not in table: {sorted(details)}")

    for url, measures in unreviewed.items():
        context.log.warning(
            "Unreviewed special measures document",
            url=url,
            measures=measures,
        )
