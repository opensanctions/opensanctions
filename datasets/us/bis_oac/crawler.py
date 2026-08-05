from csv import DictReader
from pathlib import Path
from urllib.parse import urljoin

from rigour.mime.types import CSV
from zavod import Context, settings
from zavod import helpers as h


def crawl_row(context: Context, row: dict[str, str]) -> None:
    requester = row.pop("requester").strip()
    # Skip the footer
    if "this list is provided to assist" in requester.lower():
        return
    requesting_country = row.pop("country").strip()
    date_listed = row.pop("date_listed").strip()
    if not requester and not requesting_country and not date_listed:
        return
    if not requester or not requesting_country:
        context.log.warning("Missing requester, requesting country", row=row)
        return

    entity = context.make("Company")
    entity.id = context.make_id(requester, requesting_country)
    entity.add("name", requester)
    entity.add("country", requesting_country)
    entity.add("topics", "export.risk")
    sanction = h.make_sanction(context, entity)
    h.apply_date(sanction, "listingDate", date_listed)

    context.emit(entity)
    context.emit(sanction)


def crawl_csv(context: Context, path: Path, encoding: str) -> None:
    with open(path, encoding=encoding) as fh:
        # Read the file all at once to trigger any encoding errors
        # before we start emitting entities.
        lines = fh.readlines()

    # lines[0]: "U.S. Department of Commerce
    # lines[1]: Office of Antiboycott Compliance
    # lines[2]: Requester List
    # lines[3]: (Updated March 31, 2026)",,
    assert lines[3].startswith("(Updated")
    reader = DictReader(lines[4:])
    assert reader.fieldnames is not None, "CSV file has no header row"
    # The source renames its column headers from time to time, so map them
    # onto stable keys. The lookup is required, i.e. an unknown header raises.
    fieldnames: list[str] = []
    for header in reader.fieldnames:
        column = context.lookup_value("columns", header)
        assert column is not None, header
        fieldnames.append(column)
    reader.fieldnames = fieldnames
    for row in reader:
        crawl_row(context, row)


def fetch_file_url(context: Context) -> str:
    params = {"_": settings.RUN_TIME.date().isoformat()}
    doc = context.fetch_html(context.data_url, params=params)
    return urljoin(
        context.data_url,
        h.xpath_string(doc, ".//a[text() = 'Requester List (CSV)']/@href"),
    )


def crawl(context: Context) -> None:
    context.log.info("Fetching data from the source")
    url = fetch_file_url(context)
    path = context.fetch_resource("source.csv", url)
    context.export_resource(path, CSV, title=context.SOURCE_TITLE)
    try:
        crawl_csv(context, path, "utf-8-sig")
    except UnicodeDecodeError as e:
        context.log.info(f"Failed to decode CSV file as utf-8-sig: {e}")
        # https://en.wikipedia.org/wiki/%C3%9C as DC
        crawl_csv(context, path, "cp1252")
