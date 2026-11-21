import csv
import re

from rigour.mime.types import CSV

from zavod import Context
from zavod import helpers as h

# Program key reused from 'us_trade_csl' for consistency
# Refers to the same BIS Denied Persons List program
PROGRAM_KEY = "US-BIS-DPL"
REGEX_FR_CITATION = re.compile(r"(\d+)\s*F\.?\s?R\.?\s*(\d+)")


def split_name_address(name_and_address: str) -> tuple[str, str | None]:
    """Returns (name, address), splitting on the first comma. Address is None if absent."""
    # Split only on the first comma
    parts = name_and_address.split(",", 1)
    name = parts[0].strip()
    address = parts[1].strip() if len(parts) > 1 else None
    return name, address


def parse_row(context: Context, row: dict[str, str | None]) -> None:
    checksum = h.record_hash(row)
    entity = context.make("LegalEntity")
    name_and_address = row.pop("name_and_address")
    assert name_and_address is not None
    name, address = split_name_address(name_and_address)

    entity.id = context.make_id(name, address)
    entity.add("name", name)
    entity.add("notes", row.pop("type_of_denial"))
    address_entity = h.make_address(context, full=address)
    h.copy_address(entity, address_entity)

    citation = row.pop("appropriate_federal_register_citations")
    # We don't link it to the website here, since it's included in the us_trade_csl
    # programs, which is linked to the website.
    sanction = h.make_sanction(context, entity, key=citation, program_key=PROGRAM_KEY)
    sanction.add("program", citation)
    h.apply_date(sanction, "startDate", row.pop("effective_date"))
    h.apply_date(sanction, "endDate", row.pop("expiration_date"))

    # The source has no record IDs. A party can have several rows under one order,
    # so the key needs the full name and address field; rows sharing a key would
    # alternate their checksums and look amended on every run. Renewals append FR
    # citations, so the first one names the original order.
    key = name_and_address
    first_citation = REGEX_FR_CITATION.search(citation or "")
    if first_citation is not None:
        volume, page = first_citation.groups()
        key = f"{name_and_address} {volume} FR {page}"
    h.finalize_sanction(context, sanction, key=key, checksum=checksum)

    if h.is_active(sanction):
        entity.add("topics", "sanction")

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, cache_days=1)
    urls = h.xpath_strings(
        doc, ".//a[contains(normalize-space(.), 'Export as CSV')]/@href"
    )
    assert len(urls) == 1, "Expected exactly one URL"
    url = urls[0]
    path = context.fetch_resource("source.whoknows", url)

    if ".csv" not in url:
        raise Exception(f"No known extension for {path}")
    path = path.rename(path.with_suffix(".csv"))
    with open(path, encoding="utf-8-sig") as infh:
        reader = csv.DictReader(infh)
        fieldnames = reader.fieldnames
    assert fieldnames is not None
    norm_fieldnames: list[str] = []
    for field in fieldnames:
        field = field.lower().replace(" ", "_")
        norm_field = context.lookup_value("columns", field, field)
        assert norm_field is not None
        norm_fieldnames.append(norm_field)
    with open(path, encoding="utf-8-sig") as infh:
        reader = csv.DictReader(infh, fieldnames=norm_fieldnames)
        next(reader)  # Skip header row
        rows = list(reader)

    context.export_resource(path, CSV, title=context.SOURCE_TITLE)

    for row in rows:
        row = {(k.lower()): v for k, v in row.items()}
        parse_row(context, row)
