import re
from typing import Any
from urllib.parse import urljoin

from normality import squash_spaces
from openpyxl import load_workbook
from rigour.mime.types import XLSX

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity

# Birth date written day-first inside the combined "DOB + place" field,
# e.g. "16/04/1972 à Tunis".
DOB_DMY_RE = re.compile(r"\b(\d{1,2}/\d{1,2}/\d{4})\b")
# Cells that openpyxl read as real dates arrive as ISO strings ("1972-02-21").
DOB_ISO_RE = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")
# A listing/renewal decree: "Arrêté n° 01 du 9 novembre 2018". Captures the
# decree number and the date that follows "du".
DECISION_RE = re.compile(r"n°\s*0*(\d+)\s*du\s+(\d{1,2}\s+\S+\s+\d{4})", re.IGNORECASE)
# A date introduced by "du" with a day number, used for the modification field.
MOD_DATE_RE = re.compile(r"du\s+(\d{1,2}\s+\S+\s+\d{4})", re.IGNORECASE)


def parse_birth(entity: Entity, dob_place: str) -> None:
    """Split the combined "date and place of birth" cell onto an entity.

    The field mixes a day-first date with a free-text place ("16/04/1972 à
    Tunis"), is sometimes a bare place ("Le Kef"), and is sometimes a date the
    spreadsheet stored as a real date (arriving as an ISO string). The date is
    applied to ``birthDate`` and the remainder, stripped of the French
    preposition "à", to ``birthPlace``.
    """
    dmy = DOB_DMY_RE.search(dob_place)
    iso = DOB_ISO_RE.search(dob_place)
    if dmy is not None:
        h.apply_date(entity, "birthDate", dmy.group(1))
        rest = DOB_DMY_RE.sub("", dob_place)
    elif iso is not None:
        h.apply_date(entity, "birthDate", iso.group(1))
        rest = DOB_ISO_RE.sub("", dob_place)
    else:
        rest = dob_place
    place = squash_spaces(re.sub(r"^\s*à\s+", "", rest.strip()))
    if place:
        entity.add("birthPlace", place)


def crawl_row(context: Context, row: dict[str, Any]) -> None:
    seq_num = row.pop("seq_num")
    if seq_num is None:
        return
    given_names = squash_spaces(row.pop("given_names") or "")
    surname = squash_spaces(row.pop("surname") or "")
    dob_place = squash_spaces(row.pop("dob_place") or "")
    address = squash_spaces(row.pop("address") or "")
    nationality = squash_spaces(row.pop("nationality") or "")
    reason = (row.pop("reason") or "").strip()
    decision = squash_spaces(row.pop("decision") or "")
    last_modified = squash_spaces(row.pop("last_modified") or "")
    row.pop("extra")

    # The sheet has no type column: rows with a date/place of birth are people,
    # the rest (with an empty birth field) are organizations.
    is_person = len(dob_place) > 0

    if is_person:
        entity = context.make("Person")
        entity.id = context.make_slug("person", seq_num)
    else:
        entity = context.make("Organization")
        entity.id = context.make_slug("org", seq_num)

    if not given_names and not surname:
        context.log.warning("Row with no name", seq_num=seq_num)
        return
    entity.add("topics", "sanction")
    entity.add("address", address)

    if is_person:
        # Prenom is a chain of given names, Nom the family name.
        h.apply_name(entity, first_name=given_names, last_name=surname, lang="fra")
        entity.add("nationality", nationality)
        parse_birth(entity, dob_place)
    else:
        name = " ".join(p for p in [given_names, surname] if p)
        entity.add("name", name)
        entity.add("country", nationality)

    sanction = h.make_sanction(context, entity)
    sanction.add("reason", reason)

    # Listing decree(s): "Arrêté n° 01 du 9 novembre 2018". The decree number
    # is unique per year, so we key authorityId as "<number>/<year>" and take
    # the earliest decree date as the listing date.
    if decision:
        listing_dates = []
        for m in DECISION_RE.finditer(decision):
            date_str = m.group(2)
            year = date_str.rsplit(" ", 1)[-1]
            sanction.add("authorityId", f"{int(m.group(1))}/{year}")
            listing_dates.append(date_str)
        if listing_dates:
            h.apply_date(sanction, "listingDate", listing_dates[0])
        else:
            context.log.warning("Could not parse listing decree", decision=decision)

    # Most recent renewal/modification decree carries the modification date.
    if last_modified:
        mod = MOD_DATE_RE.search(last_modified)
        if mod is not None:
            h.apply_date(sanction, "modifiedAt", mod.group(1))

    context.emit(entity)
    context.emit(sanction)
    context.audit_data(row, ignore=["pub_date"])


def find_list_url(context: Context) -> str:
    """Return the URL of the consolidated list XLSX on the CNLCT page.

    The page leads with the current consolidated list, followed by an archive
    of per-date update decrees (``MAJ_*`` files). The consolidated list is
    therefore the first XLSX link in the content body.
    """
    doc = context.fetch_html(context.data_url, cache_days=1)
    content = h.xpath_element(doc, ".//div[@class='entry-content entry clearfix']")
    for a in content.findall(".//a[@href]"):
        href = a.get("href", "")
        if href.lower().endswith(".xlsx"):
            return urljoin(context.data_url, href)
    raise ValueError("No consolidated list XLSX found on the CNLCT page")


def crawl(context: Context) -> None:
    data_url = find_list_url(context)
    path = context.fetch_resource("source.xlsx", data_url)
    context.export_resource(path, XLSX, title=context.SOURCE_TITLE)

    wb = load_workbook(path, read_only=True)
    ws = wb.active
    assert ws is not None, "No active worksheet in workbook"

    rows = list(
        h.parse_xlsx_sheet(
            context,
            ws,
            skiprows=1,  # skip the merged title row before the header row
            header_lookup=context.get_lookup("columns"),
        )
    )

    # Guard: the consolidated list is numbered contiguously from 1; the update
    # decrees carry only the sequence numbers of the entries they touch. A
    # non-contiguous sequence means we fetched an update file, not the full list.
    seqs = sorted(int(r["seq_num"]) for r in rows if r["seq_num"] is not None)
    if seqs != list(range(1, len(seqs) + 1)):
        raise ValueError(
            "Sequence numbers are not contiguous from 1 "
            f"({len(seqs)} rows) — fetched file is likely not the full list"
        )

    for row in rows:
        crawl_row(context, row)
