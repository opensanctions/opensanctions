"""Leaf utilities shared by the per-framework consolidated regulation parsers.

Everything in here is either mandated by the reviewed CSV contract
(``../data/FORMAT.md``) or by the EUR-Lex consolidation markup standard, and
is therefore identical for every parser by necessity. Parsers call these
functions top-down with plain data arguments; nothing here dispatches into
or configures parser code. Document policy — which annexes exist, which date
formats and labels are legal, per-entry quirk pins — always lives in the
individual ``parse_<celex>.py`` script. See CLAUDE.md next to this file for
the ground rules before adding anything.
"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass, field
from pathlib import Path

from followthemoney import model
from zavod.helpers.html import element_text, xpath_elements
from zavod.shed.ojeu.cellar import cli_client
from zavod.util import Element

SCRIPT_DIR = Path(__file__).resolve().parent
DATASET_DIR = SCRIPT_DIR.parent
OUTPUT_DIR = DATASET_DIR / "data" / "consolidated"

# The consolidated-file column contract from ../data/FORMAT.md.
COLUMNS = (
    "celex",
    "recordId",
    "programKey",
    "annex",
    "measure",
    "startDate",
    "reason",
    "schema",
    "name",
    "alias",
    "weakAlias",
    "previousName",
    "country",
    "nationality",
    "jurisdiction",
    "birthDate",
    "birthPlace",
    "position",
    "passportNumber",
    "gender",
    "incorporationDate",
    "legalForm",
    "registrationNumber",
    "taxNumber",
    "idNumber",
    "innCode",
    "ogrnCode",
    "kppCode",
    "okpoCode",
    "imoNumber",
    "flag",
    "address",
    "phone",
    "email",
    "website",
    "notes",
)
ENTITY_COLUMNS = COLUMNS[COLUMNS.index("name") :]

# Consolidation markup: standalone modification references ("▼M37", with an
# optional deletion dash run) and inline change markers ("►C15 value ◄").
MARKER_ROW_RE = re.compile(r"^▼(?:B|C\d+|M\d+)(?: —+)?$")
INLINE_MARKER_RE = re.compile(r"►(?:B|C\d+|M\d+) ?|◄ ?")
# "Label: value" lines inside annex cells.
LABELLED_RE = re.compile(r"^([^:]{1,40}):\s*(.*)$")
# Annex-block children that carry no entries and are skipped everywhere.
SKIP_P_CLASSES = frozenset({"", "title-annex-1", "title-annex-2"})

DATE_DOTTED_RE = re.compile(r"^(\d{1,2})\.(\d{1,2})\.(\d{4})$")
DATE_WORDED_RE = re.compile(
    r"^(\d{1,2}) (January|February|March|April|May|June|July|August"
    r"|September|October|November|December) (\d{4})$"
)
# UN-style abbreviated months carry a period ("26 Feb. 2011", "11 Sept.
# 2018"); "May" never abbreviates and is matched by the worded form above.
DATE_ABBREV_RE = re.compile(
    r"^(\d{1,2}) (Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sept?|Oct|Nov|Dec)\. (\d{4})$"
)
MONTHS = {
    "January": 1,
    "February": 2,
    "March": 3,
    "April": 4,
    "May": 5,
    "June": 6,
    "July": 7,
    "August": 8,
    "September": 9,
    "October": 10,
    "November": 11,
    "December": 12,
}
ABBREV_MONTHS = {
    "Jan": 1,
    "Feb": 2,
    "Mar": 3,
    "Apr": 4,
    "Jun": 6,
    "Jul": 7,
    "Aug": 8,
    "Sep": 9,
    "Sept": 9,
    "Oct": 10,
    "Nov": 11,
    "Dec": 12,
}


class ParseError(Exception):
    """A source structure the parser has not been taught. Fix the parser."""


@dataclass(frozen=True)
class AnnexSpec:
    """Passive description of one target annex; each parser interprets it."""

    family: str
    schema: str
    measure: str
    header: tuple[str, ...] = ()
    roles: tuple[str, ...] = ()
    parts: tuple[str, ...] = ()
    # Overrides for annexes whose parts hold different entity types.
    part_schemas: tuple[tuple[str, str], ...] = ()
    country: str = ""
    list_suffixes: bool = False  # strip trailing ";" / "; and" from list items

    def schema_for(self, part: str) -> str:
        for known, schema in self.part_schemas:
            if known == part:
                return schema
        return self.schema

    def schemata(self) -> tuple[str, ...]:
        return (self.schema, *(schema for _, schema in self.part_schemas))


@dataclass
class Row:
    """One designated entity in one legal context, pre-CSV."""

    annex: str
    schema: str
    measure: str
    record_id: str = ""
    start_date: str = ""
    reason: str = ""
    props: dict[str, list[str]] = field(default_factory=dict)

    def add(self, prop: str, values: list[str]) -> None:
        target = self.props.setdefault(prop, [])
        for value in values:
            if value and value not in target:
                target.append(value)


# --- consolidation markup and cell text ------------------------------------


def clean(text: str, ctx: str) -> str:
    """Strip inline change markers; fail on any marker char left behind."""
    out = " ".join(INLINE_MARKER_RE.sub("", text).split()).strip()
    if any(char in out for char in "►◄▼"):
        raise ParseError(f"{ctx}: unstripped marker in {out[:60]!r}")
    return out


def check_marker(text: str, ctx: str) -> None:
    if MARKER_ROW_RE.match(text) is None:
        raise ParseError(f"{ctx}: unrecognized modification marker {text!r}")


def cell_lines(td: Element, ctx: str) -> list[str]:
    """Return the cell's non-empty <p> lines, failing on stray cell text."""
    lines = [clean(element_text(p), ctx) for p in xpath_elements(td, ".//p")]
    lines = [line for line in lines if line]
    whole = clean(element_text(td), ctx)
    if " ".join(lines) != whole:
        raise ParseError(f"{ctx}: cell text outside <p> structure: {whole[:60]!r}")
    return lines


def cell_line(td: Element, ctx: str) -> str:
    """Return the single line of a scalar cell; more lines means new structure."""
    lines = cell_lines(td, ctx)
    if len(lines) != 1:
        raise ParseError(f"{ctx}: expected one line in cell, got {len(lines)}")
    return lines[0]


def bare_text(el: Element, ctx: str) -> str:
    """Text of a list entry that holds no nested structure (observed shape)."""
    if xpath_elements(el, ".//p | .//div | .//table"):
        raise ParseError(f"{ctx}: structured content in text entry")
    return clean(element_text(el), ctx)


def single_paragraph(el: Element, ctx: str) -> str:
    """Text of an entry that holds exactly one <p> and no other text."""
    paragraphs = xpath_elements(el, ".//p")
    if len(paragraphs) != 1:
        raise ParseError(f"{ctx}: entry has {len(paragraphs)} paragraphs")
    text = clean(element_text(el), ctx)
    if clean(element_text(paragraphs[0]), ctx) != text:
        raise ParseError(f"{ctx}: entry text outside <p>: {text[:60]!r}")
    return text


def split_values(value: str) -> list[str]:
    return [part.strip() for part in value.split(";") if part.strip()]


def join_multi(values: list[str]) -> str:
    """Encode values into one multi-value cell, losslessly.

    Values are "; "-joined; a value that contains the separator or a quote
    is CSV-quoted (embedded quotes doubled) so the cell decodes with a
    ";"-delimiter CSV parser, as specified in ../data/FORMAT.md.
    """
    encoded: list[str] = []
    for value in values:
        if ";" in value or '"' in value:
            value = '"' + value.replace('"', '""') + '"'
        encoded.append(value)
    return "; ".join(encoded)


# --- date primitives --------------------------------------------------------
# Each returns None on no-match so parsers compose their own fail-closed
# parse_date from exactly the formats their document has shown.


def parse_dotted_date(text: str) -> str | None:
    """ISO date from the dotted form "28.2.2011", or None."""
    match = DATE_DOTTED_RE.match(text)
    if match is None:
        return None
    day, month, year = match.groups()
    return f"{year}-{int(month):02d}-{int(day):02d}"


def parse_worded_date(text: str) -> str | None:
    """ISO date from the full-month form "2 December 1985", or None."""
    match = DATE_WORDED_RE.match(text)
    if match is None:
        return None
    return f"{match.group(3)}-{MONTHS[match.group(2)]:02d}-{int(match.group(1)):02d}"


def parse_abbrev_date(text: str) -> str | None:
    """ISO date from the UN abbreviated form "26 Feb. 2011", or None."""
    match = DATE_ABBREV_RE.match(text)
    if match is None:
        return None
    return (
        f"{match.group(3)}-{ABBREV_MONTHS[match.group(2)]:02d}"
        f"-{int(match.group(1)):02d}"
    )


# --- document access --------------------------------------------------------


def annex_blocks(doc: Element, known: set[str]) -> list[tuple[str, Element]]:
    """Locate every annex block and check the inventory the parser expects."""
    blocks: list[tuple[str, Element]] = []
    for title in xpath_elements(doc, "//p[@class='title-annex-1']"):
        text = clean(element_text(title), "annex title")
        match = re.match(r"^ANNEX ([A-Z]+)$", text)
        if match is None:
            raise ParseError(f"unrecognized annex title {text!r}")
        parent = title.getparent()
        if parent is None:
            raise ParseError(f"annex title {text!r} has no container")
        blocks.append((match.group(1), parent))
    seen = [roman for roman, _ in blocks]
    if len(set(seen)) != len(seen):
        raise ParseError("duplicate annex titles in document")
    unknown = set(seen) - known
    if unknown:
        raise ParseError(f"unknown annexes: {sorted(unknown)}")
    missing = known - set(seen)
    if missing:
        raise ParseError(f"expected annexes missing: {sorted(missing)}")
    return blocks


def assert_empty(roman: str, block: Element) -> None:
    entries = xpath_elements(
        block,
        ".//table | .//div[@class='list'] | .//div[contains(@class, 'grid-container')]",
    )
    if entries:
        raise ParseError(f"{roman}: expected-empty annex has entry content")


def annex_id(roman: str, part: str) -> str:
    return f"{roman}.{part}" if part else roman


def table_body(roman: str, table: Element, header: tuple[str, ...]) -> list[Element]:
    """Validate the header row and return data rows, skipping marker rows."""
    body: list[Element] = []
    rows = xpath_elements(table, ".//tr")
    if not rows:
        raise ParseError(f"{roman}: table has no rows")
    first = tuple(
        clean(element_text(td), roman) for td in xpath_elements(rows[0], "./td|./th")
    )
    if first != header:
        raise ParseError(f"{roman}: header {first} != expected {header}")
    for tr in rows[1:]:
        cells = xpath_elements(tr, "./td|./th")
        if len(cells) == 1:
            check_marker(" ".join(element_text(cells[0]).split()), roman)
            continue
        if len(cells) != len(header):
            raise ParseError(
                f"{roman}: row has {len(cells)} cells, expected {len(header)}"
            )
        body.append(tr)
    return body


# --- CSV assembly and output -------------------------------------------------


def to_record(row: Row, celex: str, program_key: str) -> dict[str, str]:
    """Flatten a Row into a contract record, checking columns against FtM."""
    record = {column: "" for column in COLUMNS}
    record["celex"] = celex
    record["recordId"] = row.record_id
    record["programKey"] = program_key
    record["annex"] = row.annex
    record["measure"] = row.measure
    record["startDate"] = row.start_date
    record["reason"] = row.reason
    record["schema"] = row.schema
    schema = model.get(row.schema)
    if schema is None:
        raise ParseError(f"{row.annex}: unknown schema {row.schema!r}")
    for prop, values in row.props.items():
        if prop not in ENTITY_COLUMNS:
            raise ParseError(f"{row.annex}: {prop!r} is not a CSV column")
        if prop not in schema.properties:
            raise ParseError(f"{row.annex}: {row.schema} has no {prop!r}")
        if prop == "name":
            # name is the one scalar entity column: never joined, never split.
            if len(values) != 1:
                raise ParseError(f"{row.annex}: {len(values)} name values")
            record[prop] = values[0]
        else:
            record[prop] = join_multi(values)
    if not record["name"]:
        raise ParseError(f"{row.annex}: row without name")
    return record


def validate_records(records: list[dict[str, str]]) -> None:
    seen_rows: set[tuple[str, ...]] = set()
    seen_ids: set[tuple[str, str]] = set()
    for record in records:
        key = tuple(record[column] for column in COLUMNS)
        if key in seen_rows:
            raise ParseError(f"{record['annex']}: duplicate row {record['name']!r}")
        seen_rows.add(key)
        if record["recordId"]:
            id_key = (record["annex"], record["recordId"])
            if id_key in seen_ids:
                raise ParseError(
                    f"{record['annex']}: duplicate entry number {record['recordId']}"
                )
            seen_ids.add(id_key)


def summary(records: list[dict[str, str]], celex: str) -> dict[str, object]:
    """Per-annex parity counts, echoed for the agent re-running the script."""
    counts: dict[str, int] = {}
    for record in records:
        counts[record["annex"]] = counts.get(record["annex"], 0) + 1
    return {
        "consolidated_celex": celex,
        "rows": counts,
        "blank_start_dates": sum(1 for r in records if not r["startDate"]),
        "total": len(records),
    }


def write_csv(records: list[dict[str, str]], framework_celex: str) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = OUTPUT_DIR / f"{framework_celex}.csv"
    with open(csv_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(COLUMNS))
        writer.writeheader()
        writer.writerows(records)
    return csv_path


# --- source retrieval --------------------------------------------------------


def load_source(celex: str, source: Path | None) -> bytes:
    if source is not None:
        return source.read_bytes()
    cached = SCRIPT_DIR / "out" / f"{celex}.xhtml"
    if cached.exists():
        return cached.read_bytes()
    with cli_client() as client:
        expression = client.fetch_expression(celex)
    cached.parent.mkdir(parents=True, exist_ok=True)
    cached.write_bytes(expression.content)
    return expression.content
