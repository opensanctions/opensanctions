"""Parse consolidated Regulation (EU) 833/2014 into a reviewed candidate CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

Output: data/consolidated/32014R0833.csv (the EU Journal consolidated CSV
contract, keyed by the framework act). The consolidated version the snapshot
was extracted from is passed as the CELEX argument and pinned in the dataset
YAML's `consolidation` lookup, updated in the same commit as the CSV.
"""

from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, get_args

import click
from followthemoney import model
from lxml import html
from zavod.helpers.html import element_text, xpath_elements
from zavod.shed.ojeu.cellar import cli_client
from zavod.stateful.programs import Measure, get_program_by_key
from zavod.util import Element

FRAMEWORK_CELEX = "32014R0833"
CONSOLIDATED_RE = re.compile(r"^02014R0833-\d{8}$")
PROGRAM_KEY = "EU-RUS"

SCRIPT_DIR = Path(__file__).resolve().parent
DATASET_DIR = SCRIPT_DIR.parent
OUTPUT_DIR = DATASET_DIR / "data" / "consolidated"

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
)
ENTITY_COLUMNS = COLUMNS[COLUMNS.index("name") :]

# Consolidation markup: standalone modification references ("▼M37", with an
# optional deletion dash run) and inline change markers ("►C15 value ◄").
MARKER_ROW_RE = re.compile(r"^▼(?:B|C\d+|M\d+)(?: —+)?$")
INLINE_MARKER_RE = re.compile(r"►(?:B|C\d+|M\d+) ?|◄ ?")
PART_RE = re.compile(r"^Part ([A-Z])(?: – .+)?$")
# The dot is missing on one observed XLII entry (648).
NUMBER_RE = re.compile(r"^(\d+)\.?$")
NUMBERED_NAME_RE = re.compile(r"^(\d+)\.\s+(\S.*)$")
LABELLED_RE = re.compile(r"^([^:]{1,40}):\s*(.*)$")
DATE_DOTTED_RE = re.compile(r"^(\d{1,2})\.(\d{1,2})\.(\d{4})$")
DATE_WORDED_RE = re.compile(
    r"^(\d{1,2}) (January|February|March|April|May|June|July|August"
    r"|September|October|November|December) (\d{4})$"
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
FORMERLY_RE = re.compile(r"^\(formerly (.+)\)$")
# Former vessel names also appear inline: "Kavya (formerly Hana)"; one
# observed entry (XLII 218) never closes the parenthesis.
INLINE_FORMERLY_RE = re.compile(r"^(.+?) \(formerly ([^()]+)\)?$")
# Alias labels also occur embedded mid-value: "Local name: X A.k.a: Y".
EMBEDDED_ALIAS_RE = re.compile(
    r"\s(?:Local names?|Local Name|A\.k\.a\.?|a\.k\.a\.|Aka|Alias(?:es)?"
    r"|Alsiases):\s*"
)

# Annex IV name-cell labels; every observed variant maps to alias.
NAME_ALIAS_LABELS = frozenset(
    {
        "Local name",
        "Local Name",
        "Local names",
        "A.k.a",
        "A.k.a.",
        "a.k.a.",
        "Aka",
        "Alias",
        "Alsiases",
        "Chinese company name",
        "Shenzhen branch",
    }
)
# Annex IV identifying-information labels → FtM property.
ID_LABELS = {
    "Address(es)": "address",
    "Address": "address",
    "Registration number": "registrationNumber",
    "Registration Number": "registrationNumber",
    "Website": "website",
    "Websites": "website",
    "Telephone": "phone",
    "Telephones": "phone",
    "Phone": "phone",
    "Email": "email",
    "Emails": "email",
    "E-mail": "email",
    "email": "email",
    "Place of registration": "jurisdiction",
}

Family = Literal["plain_list", "grid_list", "numbered_list", "table"]
# Cell roles for table annexes, in column order.
Role = Literal[
    "recordId",
    "name",
    "startDate",
    "reason",
    "address",
    "iv_name",
    "iv_info",
    "vessel_name",
    "imoNumber",
]


class ParseError(Exception):
    """A source structure this parser has not been taught. Fix the code."""


@dataclass(frozen=True)
class AnnexSpec:
    family: Family
    schema: str
    measure: Measure
    header: tuple[str, ...] = ()
    roles: tuple[Role, ...] = ()
    parts: tuple[str, ...] = ()
    country: str = ""
    list_suffixes: bool = False  # strip trailing ";" / "; and" from list items


ENTITY_TABLE = "Name of the legal person, entity or body"
TARGETS: dict[str, AnnexSpec] = {
    "III": AnnexSpec("grid_list", "LegalEntity", "Financial restrictions"),
    "IV": AnnexSpec(
        "table",
        "LegalEntity",
        "Export control",
        header=("Number", "Name", "Identifying Information", "Date of listing"),
        roles=("recordId", "iv_name", "iv_info", "startDate"),
    ),
    "V": AnnexSpec("plain_list", "LegalEntity", "Financial restrictions"),
    "VI": AnnexSpec("plain_list", "LegalEntity", "Financial restrictions"),
    "XII": AnnexSpec(
        "plain_list",
        "LegalEntity",
        "Financial restrictions",
        list_suffixes=True,
    ),
    "XIII": AnnexSpec(
        "plain_list",
        "LegalEntity",
        "Financial restrictions",
        list_suffixes=True,
    ),
    "XIV": AnnexSpec(
        "table",
        "LegalEntity",
        "Financial restrictions",
        header=(ENTITY_TABLE, "Date of application"),
        roles=("name", "startDate"),
    ),
    "XV": AnnexSpec("plain_list", "LegalEntity", "Services ban"),
    "XIX": AnnexSpec(
        "numbered_list",
        "LegalEntity",
        "Financial restrictions",
        parts=("A", "B", "C"),
    ),
    "XLII": AnnexSpec(
        "table",
        "Vessel",
        "Transportation restrictions",
        header=(
            "",
            "Vessel name",
            "IMO number",
            "Grounds for inclusion",
            "Date of application",
        ),
        roles=("recordId", "vessel_name", "imoNumber", "reason", "startDate"),
    ),
    "XLIV": AnnexSpec(
        "table",
        "LegalEntity",
        "Financial restrictions",
        header=("", ENTITY_TABLE, "Entry into force"),
        roles=("recordId", "name", "startDate"),
    ),
    "XLV": AnnexSpec(
        "table",
        "LegalEntity",
        "Financial restrictions",
        header=(ENTITY_TABLE, "Entry into force"),
        roles=("name", "startDate"),
        parts=("A", "B", "C", "D"),
    ),
    "XLVII": AnnexSpec(
        "table",
        "Asset",
        "Transportation restrictions",
        header=("", "Name", "Grounds for inclusion", "Date of application"),
        roles=("recordId", "name", "reason", "startDate"),
        parts=("A", "B", "C"),
    ),
    "XLIX": AnnexSpec(
        "table",
        "LegalEntity",
        "Financial restrictions",
        header=(
            "Name of listed legal person, entity or body",
            "Place of registration",
            "Entry into force",
        ),
        roles=("name", "address", "startDate"),
    ),
    "LII": AnnexSpec(
        "table",
        "Asset",
        "Investment ban",
        header=("Number", "Name", "Location"),
        roles=("recordId", "name", "address"),
        parts=("A", "B"),
        country="Russia",
    ),
    "LIII": AnnexSpec(
        "table",
        "Asset",
        "Financial restrictions",
        header=("Crypto-assets or central bank digital currencies", "Entry into force"),
        roles=("name", "startDate"),
    ),
}

EXPECTED_EMPTY = frozenset({"XLIII", "XLVI", "L", "LIV", "LV", "LVI"})
NON_TARGET = frozenset(
    {
        "I",
        "II",
        "VII",
        "VIII",
        "IX",
        "X",
        "XI",
        "XVI",
        "XVII",
        "XVIII",
        "XX",
        "XXI",
        "XXIII",
        "XXIIIH",
        "XXIV",
        "XXV",
        "XXVI",
        "XXVII",
        "XXVIII",
        "XXIX",
        "XXX",
        "XXXI",
        "XXXII",
        "XXXIII",
        "XXXV",
        "XXXVI",
        "XXXVII",
        "XXXVIIIA",
        "XXXVIIIB",
        "XXXIX",
        "XL",
        "XLI",
        "XLVIII",
        "LI",
    }
)

# Annex-block children that carry no entries and are skipped everywhere.
SKIP_P_CLASSES = frozenset({"", "title-annex-1", "title-annex-2"})


@dataclass
class Row:
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


def clean(text: str, ctx: str) -> str:
    """Strip inline change markers; fail on any marker char left behind."""
    out = " ".join(INLINE_MARKER_RE.sub("", text).split()).strip()
    if any(char in out for char in "►◄▼"):
        raise ParseError(f"{ctx}: unstripped marker in {out[:60]!r}")
    return out


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


def parse_date(text: str, ctx: str) -> str:
    dotted = DATE_DOTTED_RE.match(text)
    if dotted is not None:
        day, month, year = dotted.groups()
        return f"{year}-{int(month):02d}-{int(day):02d}"
    worded = DATE_WORDED_RE.match(text)
    if worded is not None:
        return (
            f"{worded.group(3)}-{MONTHS[worded.group(2)]:02d}"
            f"-{int(worded.group(1)):02d}"
        )
    raise ParseError(f"{ctx}: unrecognized date {text!r}")


def parse_record_id(text: str, ctx: str) -> str:
    match = NUMBER_RE.match(text)
    if match is None:
        raise ParseError(f"{ctx}: unrecognized entry number {text!r}")
    return match.group(1)


def annex_blocks(doc: Element) -> list[tuple[str, Element]]:
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
    known = set(TARGETS) | EXPECTED_EMPTY | NON_TARGET
    unknown = set(seen) - known
    if unknown:
        raise ParseError(f"unknown annexes: {sorted(unknown)}")
    missing = known - set(seen)
    if missing:
        raise ParseError(f"expected annexes missing: {sorted(missing)}")
    return blocks


def check_marker(text: str, ctx: str) -> None:
    if MARKER_ROW_RE.match(text) is None:
        raise ParseError(f"{ctx}: unrecognized modification marker {text!r}")


def assert_empty(roman: str, block: Element) -> None:
    entries = xpath_elements(
        block,
        ".//table | .//div[@class='list'] | .//div[contains(@class, 'grid-container')]",
    )
    if entries:
        raise ParseError(f"{roman}: expected-empty annex has entry content")


def iter_entry_children(
    roman: str, block: Element, parts: tuple[str, ...]
) -> list[tuple[str, Element]]:
    """Walk direct children; bind entry elements to their current part.

    Returns (part, element) pairs for entry-bearing children: div.centered
    table containers, div.list items, grid containers, and bare entry divs.
    """
    entries: list[tuple[str, Element]] = []
    part = ""
    seen_parts: list[str] = []
    for child in block.iterchildren():
        if not isinstance(child.tag, str):
            continue
        cls = child.get("class") or ""
        if child.tag == "hr" and cls == "separator-annex":
            continue
        if child.tag == "p":
            text = clean(element_text(child), roman) if cls != "modref" else ""
            if cls == "modref":
                check_marker(" ".join(element_text(child).split()), roman)
                continue
            if cls in SKIP_P_CLASSES:
                continue
            if cls in ("norm", "title-gr-seq-level-1"):
                match = PART_RE.match(text)
                if match is not None:
                    part = match.group(1)
                    seen_parts.append(part)
                    if not parts:
                        raise ParseError(f"{roman}: unexpected part {text!r}")
                # Non-part norm/heading text is annex prose; carries no rows.
                continue
            raise ParseError(f"{roman}: unexpected <p class={cls!r}> {text[:50]!r}")
        if child.tag == "div":
            entries.append((part, child))
            continue
        raise ParseError(f"{roman}: unexpected element <{child.tag}>")
    if tuple(seen_parts) != parts:
        raise ParseError(f"{roman}: parts {seen_parts} != expected {list(parts)}")
    return entries


def annex_id(roman: str, part: str) -> str:
    return f"{roman}.{part}" if part else roman


# --- parser families ---------------------------------------------------


def parse_plain_list(roman: str, spec: AnnexSpec, block: Element) -> list[Row]:
    rows: list[Row] = []
    for part, div in iter_entry_children(roman, block, spec.parts):
        if div.get("class") != "list":
            raise ParseError(f"{roman}: unexpected div class {div.get('class')!r}")
        name = bare_text(div, roman)
        if spec.list_suffixes:
            name = name.removesuffix("; and").removesuffix(";").strip()
        if not name:
            raise ParseError(f"{roman}: empty list item")
        row = Row(annex_id(roman, part), spec.schema, spec.measure)
        row.add("name", [name])
        rows.append(row)
    return rows


def parse_grid_list(roman: str, spec: AnnexSpec, block: Element) -> list[Row]:
    rows: list[Row] = []
    for part, div in iter_entry_children(roman, block, spec.parts):
        if "grid-container" not in (div.get("class") or ""):
            raise ParseError(f"{roman}: unexpected div class {div.get('class')!r}")
        cells = [c for c in div.iterchildren() if isinstance(c.tag, str)]
        if len(cells) != 2:
            raise ParseError(f"{roman}: grid entry has {len(cells)} columns")
        record_id = parse_record_id(bare_text(cells[0], roman), roman)
        name = single_paragraph(cells[1], roman)
        if not name:
            raise ParseError(f"{roman}: empty name for entry {record_id}")
        row = Row(annex_id(roman, part), spec.schema, spec.measure, record_id=record_id)
        row.add("name", [name])
        rows.append(row)
    return rows


def parse_numbered_list(roman: str, spec: AnnexSpec, block: Element) -> list[Row]:
    rows: list[Row] = []
    for part, div in iter_entry_children(roman, block, spec.parts):
        if not part:
            raise ParseError(f"{roman}: entry before first part heading")
        text = single_paragraph(div, roman)
        match = NUMBERED_NAME_RE.match(text)
        if match is None:
            raise ParseError(f"{roman}: unrecognized entry {text[:60]!r}")
        row = Row(
            annex_id(roman, part), spec.schema, spec.measure, record_id=match.group(1)
        )
        row.add("name", [match.group(2)])
        rows.append(row)
    return rows


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


def parse_table(roman: str, spec: AnnexSpec, block: Element) -> list[Row]:
    rows: list[Row] = []
    for part, div in iter_entry_children(roman, block, spec.parts):
        if div.get("class") != "centered":
            raise ParseError(f"{roman}: unexpected div class {div.get('class')!r}")
        table = xpath_elements(div, ".//table", expect_exactly=1)[0]
        if spec.parts and not part:
            raise ParseError(f"{roman}: table before first part heading")
        for tr in table_body(roman, table, spec.header):
            rows.append(parse_table_row(roman, part, spec, tr))
    return rows


def parse_table_row(roman: str, part: str, spec: AnnexSpec, tr: Element) -> Row:
    row = Row(annex_id(roman, part), spec.schema, spec.measure)
    cells = xpath_elements(tr, "./td|./th")
    ctx = roman
    for role, td in zip(spec.roles, cells, strict=True):
        if role == "recordId":
            row.record_id = parse_record_id(cell_line(td, ctx), ctx)
            ctx = f"{roman} entry {row.record_id}"
        elif role == "name":
            row.add("name", [cell_line(td, ctx)])
        elif role == "startDate":
            row.start_date = parse_date(cell_line(td, ctx), ctx)
        elif role == "reason":
            # Grounds cells legitimately span paragraphs (XLII, XLVII).
            row.reason = " ".join(cell_lines(td, ctx))
        elif role == "address":
            row.add("address", [cell_line(td, ctx)])
        elif role == "imoNumber":
            imo = cell_line(td, ctx)
            if re.match(r"^\d{7}$", imo) is None:
                raise ParseError(f"{ctx}: unrecognized IMO number {imo!r}")
            row.add("imoNumber", [imo])
        elif role == "vessel_name":
            parse_vessel_name(ctx, cell_lines(td, ctx), row)
        elif role == "iv_name":
            parse_iv_name(ctx, cell_lines(td, ctx), row)
        elif role == "iv_info":
            parse_iv_info(ctx, cell_lines(td, ctx), row)
    if spec.country:
        row.add("country", [spec.country])
    return row


def parse_vessel_name(ctx: str, lines: list[str], row: Row) -> None:
    if not lines:
        raise ParseError(f"{ctx}: empty vessel name cell")
    inline = INLINE_FORMERLY_RE.match(lines[0])
    if inline is not None:
        row.add("name", [inline.group(1)])
        row.add("previousName", split_values(inline.group(2)))
    else:
        row.add("name", [lines[0]])
    for line in lines[1:]:
        match = FORMERLY_RE.match(line)
        if match is None:
            raise ParseError(f"{ctx}: unrecognized name line {line!r}")
        row.add("previousName", split_values(match.group(1)))


# --- Annex IV cell grammars ---------------------------------------------


# Unlabelled continuation lines that neither start with "(" nor carry a
# label, pinned by Annex IV entry number and line prefix. Any other
# unlabelled line raises for review instead of merging silently.
IV_CONTINUATIONS = (
    ("594", "Economic Zone (SEZ);"),
    ("148", "jsc-energiya.com/"),
)
# Annex IV entries whose identifying information opens with a bare,
# unlabelled address line (observed once).
IV_BARE_FIRST_ADDRESS = frozenset({"402"})


def merge_labelled_lines(
    ctx: str,
    record_id: str,
    lines: list[str],
    known: frozenset[str] | dict[str, str],
) -> list[str]:
    """Merge pinned continuation lines into their labelled predecessor.

    A line opens a new field only with a known "Label:" prefix; an unknown
    label raises. A label-less line merges into the previous line only when
    it visibly belongs to it — parenthesized, or pinned in IV_CONTINUATIONS —
    and raises otherwise.
    """
    merged: list[str] = []
    for line in lines:
        match = LABELLED_RE.match(line)
        if match is not None and not line.startswith("("):
            if match.group(1) in known:
                merged.append(line)
                continue
            raise ParseError(f"{ctx}: unknown label {match.group(1)!r}")
        continuation = line.startswith("(") or any(
            entry == record_id and line.startswith(prefix)
            for entry, prefix in IV_CONTINUATIONS
        )
        if not merged or not continuation:
            raise ParseError(f"{ctx}: unlabelled line {line[:60]!r}")
        merged[-1] += " " + line
    return merged


def split_aliases(value: str) -> list[str]:
    parts = EMBEDDED_ALIAS_RE.split(value)
    return [alias for part in parts for alias in split_values(part)]


def parse_iv_name(ctx: str, lines: list[str], row: Row) -> None:
    if not lines:
        raise ParseError(f"{ctx}: empty name cell")
    first = lines[0]
    # Observed once: "<name> Local name: <local>" on the first line.
    if " Local name:" in first:
        name, _, local = first.partition(" Local name:")
        row.add("name", [name.strip()])
        row.add("alias", split_aliases(local))
    else:
        row.add("name", [first])
    for line in merge_labelled_lines(ctx, row.record_id, lines[1:], NAME_ALIAS_LABELS):
        match = LABELLED_RE.match(line)
        if match is None or match.group(1) not in NAME_ALIAS_LABELS:
            raise ParseError(f"{ctx}: unlabelled name line {line[:60]!r}")
        row.add("alias", split_aliases(match.group(2)))


def parse_iv_info(ctx: str, lines: list[str], row: Row) -> None:
    # Observed twice: "Website http://…" missing its colon.
    fixed = [re.sub(r"^Website (https?://)", r"Website: \1", line) for line in lines]
    # Observed once: a first line that is a bare, unlabelled address.
    if fixed and LABELLED_RE.match(fixed[0]) is None:
        if row.record_id not in IV_BARE_FIRST_ADDRESS:
            raise ParseError(f"{ctx}: unlabelled first info line {fixed[0][:60]!r}")
        fixed[0] = "Address(es): " + fixed[0]
    for line in merge_labelled_lines(ctx, row.record_id, fixed, ID_LABELS):
        match = LABELLED_RE.match(line)
        if match is None or match.group(1) not in ID_LABELS:
            raise ParseError(f"{ctx}: unlabelled info line {line[:60]!r}")
        row.add(ID_LABELS[match.group(1)], split_values(match.group(2)))


FAMILIES = {
    "plain_list": parse_plain_list,
    "grid_list": parse_grid_list,
    "numbered_list": parse_numbered_list,
    "table": parse_table,
}


# --- assembly and validation ---------------------------------------------


def check_registry() -> None:
    program = get_program_by_key(PROGRAM_KEY)
    if program is None:
        raise ParseError(f"unknown program key {PROGRAM_KEY!r}")
    for roman, spec in TARGETS.items():
        if spec.measure not in get_args(Measure):
            raise ParseError(f"{roman}: invalid measure {spec.measure!r}")
        if spec.measure not in program.measures:
            raise ParseError(f"{roman}: measure not in {PROGRAM_KEY}")
        schema = model.get(spec.schema)
        if schema is None:
            raise ParseError(f"{roman}: unknown schema {spec.schema!r}")


def parse_document(doc: Element) -> list[Row]:
    rows: list[Row] = []
    for roman, block in annex_blocks(doc):
        if roman in NON_TARGET:
            continue
        if roman in EXPECTED_EMPTY:
            assert_empty(roman, block)
            continue
        spec = TARGETS[roman]
        annex_rows = FAMILIES[spec.family](roman, spec, block)
        if not annex_rows:
            raise ParseError(f"{roman}: no entries extracted")
        rows.extend(annex_rows)
    return rows


def to_record(row: Row) -> dict[str, str]:
    record = {column: "" for column in COLUMNS}
    record["celex"] = FRAMEWORK_CELEX
    record["recordId"] = row.record_id
    record["programKey"] = PROGRAM_KEY
    record["annex"] = row.annex
    record["measure"] = row.measure
    record["startDate"] = row.start_date
    record["reason"] = row.reason
    record["schema"] = row.schema
    schema = model.get(row.schema)
    assert schema is not None
    for prop, values in row.props.items():
        if prop not in ENTITY_COLUMNS:
            raise ParseError(f"{row.annex}: {prop!r} is not a CSV column")
        if prop not in schema.properties:
            raise ParseError(f"{row.annex}: {row.schema} has no {prop!r}")
        record[prop] = "; ".join(values)
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


# --- source retrieval and CLI ---------------------------------------------


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


@click.command(help="Parse consolidated Regulation 833/2014 into a CSV candidate.")
@click.argument("celex")
@click.option(
    "--source",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    help="Parse these exact XHTML bytes instead of fetching from CELLAR.",
)
def main(celex: str, source: Path | None) -> None:
    try:
        check_registry()
        if CONSOLIDATED_RE.match(celex) is None:
            raise ParseError(f"not a consolidated 833/2014 CELEX: {celex!r}")
        content = load_source(celex, source)
        doc = html.fromstring(content)
        rows = parse_document(doc)
        records = [to_record(row) for row in rows]
        validate_records(records)

        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        csv_path = OUTPUT_DIR / f"{FRAMEWORK_CELEX}.csv"
        with open(csv_path, "w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(COLUMNS))
            writer.writeheader()
            writer.writerows(records)
        click.echo(json.dumps(summary(records, celex), indent=2))
        click.echo(f"wrote {csv_path}")
    except ParseError as exc:
        raise click.ClickException(str(exc)) from exc


if __name__ == "__main__":
    main()
