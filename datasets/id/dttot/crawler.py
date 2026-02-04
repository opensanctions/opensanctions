import re
import openpyxl
from typing import Dict
from normality import squash_spaces
from rigour.mime.types import XLSX

from zavod import Context, helpers as h

# Date range like "00/00/1970-1973"
DATE_RANGE_RE = re.compile(r"00/00/(\d{4})-(\d{4})")
ADDR_DELIM = re.compile(r"[\W][a-zA-Z]\)|;")
# eng: 'Branch office 4:'
BRANCH_PATTERN = re.compile(r"^kantor cabang \d+[,:]\s*", re.IGNORECASE)


def clean_addresses(raw_addresses: str) -> list[str]:
    """Split and clean a multi-address string into individual addresses."""
    cleaned = []
    for address_block in h.multi_split(
        raw_addresses, ["\n-\t", "\n- ", ", - ", ", -,"]
    ):
        for address in ADDR_DELIM.split(address_block):
            address = squash_spaces(BRANCH_PATTERN.sub("", address).strip("-"))
            if address and not BRANCH_PATTERN.match(address):
                cleaned.append(address)
    return cleaned


def crawl_row(context: Context, row: Dict[str, str | None]):
    item_id = row.pop("id")
    res = context.lookup("type", row.pop("type"), warn_unmatched=True)
    assert res and res.value
    entity = context.make(res.value)
    names = h.multi_split(row.pop("name"), ["alias", "ALIAS"])
    entity.id = context.make_id(item_id, *names)
    entity.add("name", names[0])
    entity.add("alias", names[1:])
    entity.add("topics", "sanction")
    for c in h.multi_split(row.pop("country"), ["\n"]):
        entity.add("country", c.strip("-"))
    if notes := row.pop("description"):
        entity.add("notes", squash_spaces(notes))
    if raw_addresses := row.pop("address"):
        for address in clean_addresses(raw_addresses):
            entity.add("address", address)

    dob_raw = row.pop("birth_date")
    birth_place = row.pop("birth_place")
    if entity.schema.is_a("Person"):
        entity.add("birthPlace", birth_place)
        for dob in h.multi_split(dob_raw, [" atau ", "\n", "- ", " / "]):
            if match := DATE_RANGE_RE.match(dob):
                # Date range like "00/00/1970-1973" - add both years
                for dob in list(match.groups()):
                    h.apply_date(entity, "birthDate", dob)
            h.apply_date(entity, "birthDate", dob)

    sanction = h.make_sanction(context, entity)
    sanction.add("authorityId", item_id)

    context.emit(entity)
    context.emit(sanction)

    context.audit_data(row)


def crawl(context: Context):
    doc = context.fetch_html(context.data_url)
    xlsx_link = h.xpath_string(
        doc,
        ".//p[contains(., 'Daftar Terduga Teroris')]//a[contains(@href, '.xlsx')]/@href",
    )
    path = context.fetch_resource("source.xlsx", xlsx_link)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)
    workbook: openpyxl.Workbook = openpyxl.load_workbook(path, read_only=True)
    for row in h.parse_xlsx_sheet(
        context, workbook["Export"], header_lookup=context.get_lookup("headers")
    ):
        crawl_row(context, row)
