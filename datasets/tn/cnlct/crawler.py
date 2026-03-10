import re
from typing import Any
from urllib.parse import urljoin

from normality import squash_spaces
from openpyxl import load_workbook
from rigour.mime.types import XLSX

from zavod import Context
from zavod import helpers as h

# Matches YYYY/MM/DD or YYYY-MM-DD embedded in combined DOB+place field
DOB_DATE_RE = re.compile(r"(\d{4}[/-]\d{2}[/-]\d{2})")
# Matches the date substring following Arabic "مؤرخ في" (dated on)
DECISION_DATE_RE = re.compile(r"مؤرخ في\s+(.+?)(?:\s{3,}|قرار|$)", re.DOTALL)
# Matches decision numbers: "عدد 01 لسنة 2026"
DECISION_NUM_RE = re.compile(r"عدد\s+0*(\d+)\s+لسنة\s+(\d{4})")


def extract_decision_dates(text: str) -> list[str]:
    """Return all date strings following 'مؤرخ في' in a decision field."""
    return [m.group(1).strip() for m in DECISION_DATE_RE.finditer(text)]


def crawl_row(context: Context, row: dict[str, Any]) -> None:
    seq_num = row.pop("seq_num")
    if seq_num is None:
        return
    full_name = squash_spaces(row.pop("full_name") or "")
    surname = squash_spaces(row.pop("surname") or "")
    dob_place = row.pop("dob_place")
    address = squash_spaces(row.pop("address") or "")
    nationality = row.pop("nationality")

    last_modified = squash_spaces(row.pop("last_modified") or "")
    row.pop("extra")

    # Determine entity type: Person if DOB field is present, Organization otherwise
    is_person = dob_place is not None and len(str(dob_place))

    if is_person:
        entity = context.make("Person")
        entity.id = context.make_slug("person", seq_num)
    else:
        entity = context.make("Organization")
        entity.id = context.make_slug("org", seq_num)

    if not full_name and not surname:
        context.log.warning("Row with no name", seq_num=seq_num)
        return
    entity.add("topics", "sanction")
    entity.add("address", address)

    if is_person:
        # الإسم الثلاثي = 3-part given-name chain; اللقب = family surname
        h.apply_name(entity, first_name=full_name, last_name=surname)
        entity.add("nationality", nationality)
        # Parse DOB from combined "YYYY/MM/DD ب[city]" field
        dob_str = str(dob_place).strip()
        dob_m = DOB_DATE_RE.search(dob_str)
        if dob_m:
            # Normalise YYYY-MM-DD → YYYY/MM/DD for h.apply_date format matching
            date_str = dob_m.group(1).replace("-", "/")
            h.apply_date(entity, "birthDate", date_str)
        # Birth place: remove the date and leading Arabic preposition "ب" (meaning
        # "in"/"at").  Only the single preposition letter is stripped so that the
        # definite article "ال" stays attached to place names that carry it
        # (e.g. "بالمنستير" → "المنستير", not "منستير").
        birth_place = DOB_DATE_RE.sub("", dob_str).strip()
        birth_place = re.sub(r"^ب", "", birth_place).strip()
        if birth_place:
            entity.add("birthPlace", birth_place)
    else:
        name = " ".join(p for p in [full_name, surname] if p)
        entity.add("name", name)
        entity.add("country", nationality)

    sanction = h.make_sanction(context, entity)
    sanction.add("reason", row.pop("reason"))

    decision = squash_spaces(row.pop("decision") or "")
    # Authority IDs from listing decision(s)
    if len(decision):
        for m in DECISION_NUM_RE.finditer(decision):
            sanction.add("authorityId", f"{m.group(1)}/{m.group(2)}")
        listing_dates = extract_decision_dates(decision)
        if listing_dates:
            h.apply_date(sanction, "listingDate", listing_dates[0])

    # Last modification date
    if len(last_modified):
        mod_dates = extract_decision_dates(last_modified)
        if mod_dates:
            h.apply_date(sanction, "modifiedAt", mod_dates[0])

    context.emit(entity)
    context.emit(sanction)
    context.audit_data(row, ignore=["pub_date", "extra"])


def crawl(context: Context) -> None:
    # Scrape landing page to find the most recent complete list XLSX.
    # Full list URLs never contain "تحيين" (update) or "حذف" (deletion).
    doc = context.fetch_html(context.data_url, cache_days=1)
    data_url: str | None = None
    for a in doc.findall(".//a[@href]"):
        href = a.get("href", "")
        if not href.lower().endswith(".xlsx"):
            continue
        filename = href.rsplit("/", 1)[-1]
        # Skip if filename contains "تحيين" (update) or "حذف" (deletion): not full lists
        if "تحيين" in filename or "حذف" in filename:
            continue
        data_url = urljoin(context.data_url, href)
        break

    if data_url is None:
        raise ValueError("No full-list XLSX found on the CNLCT page")

    path = context.fetch_resource("source.xlsx", data_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)
    ws = wb.active
    assert ws is not None, "No active worksheet in workbook"

    for item in h.parse_xlsx_sheet(
        context,
        ws,
        skiprows=1,  # skip merged title row before the header row
        header_lookup=context.get_lookup("columns"),
    ):
        crawl_row(context, item)
