"""Parse consolidated Regulation (EU) 2023/1529 (Iran military support) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation's annexes:

- Annex I — competent-authority websites, not designations.
- Annex II — the Article 2 list of UAV- and missile-programme items, goods
  only.
- Annex III — the Article 3 fund-freeze list, with parts A. Natural persons
  and B. Legal persons, entities and bodies, each printed as one six-column
  table (entry number, Latin-script name, native-script name, identifying
  information, reasons, date of listing). Travel bans live in Decision
  (CFSP) 2023/1532, not in this regulation.
- Annex IV — the Article 2a list of ports and locks, locations only.

Farsi renderings in the native-script column print as one rendering line
plus a "(Farsi spelling)" annotation; several entries render the Farsi text
as an embedded image instead, which cannot be transcribed and yields no
alias (the annotation line alone is accepted). One entry continues into a
second table row whose number and name cells are empty; the continuation's
identifying information belongs to the preceding entry. Relational
"Associated …" lines name other parties, have no CSV column, and are
deliberately not transcribed. Dates are transcribed as the source prints
them ("11.12.2023"); the crawler normalizes dates.

Output: data/consolidated/32023R1529.csv (the EU Journal consolidated CSV
contract, keyed by the framework act). The consolidated version the snapshot
was extracted from is passed as the CELEX argument and pinned in the dataset
YAML's `consolidation` lookup, updated in the same commit as the CSV.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import get_args

import click
from common import (
    LABELLED_RE,
    SKIP_P_CLASSES,
    ParseError,
    Row,
    annex_blocks,
    annex_id,
    cell_line,
    cell_lines,
    check_marker,
    clean,
    load_source,
    parse_dotted_date,
    split_values,
    summary,
    table_body,
    to_record,
    validate_records,
    write_csv,
)
from followthemoney import model
from lxml import html
from zavod.helpers.html import element_text, xpath_elements
from zavod.stateful.programs import Measure, get_program_by_key
from zavod.util import Element

FRAMEWORK_CELEX = "32023R1529"
CONSOLIDATED_RE = re.compile(r"^02023R1529-\d{8}$")
PROGRAM_KEY = "EU-IRN"
# Annex III implements the regulation's Article 3 fund freeze; travel bans
# live in Decision (CFSP) 2023/1532.
MEASURE = "Asset freeze"

TARGET_ANNEX = "III"
NON_TARGET = frozenset({"I", "II", "IV"})

# Annex III prints each part as a grid-list: the letter in column 1, the
# part heading and one entry table in column 2. Both parts share one header.
PART_HEADER = (
    "",
    "Names (Transliteration into Latin script)",
    "Names",
    "Identifying information",
    "Reasons for listing",
    "Date of listing",
)
# (letter cell, part heading, part id, schema) in print order.
PARTS = (
    ("A.", "Natural persons", "A", "Person"),
    ("B.", "Legal persons, entities and bodies", "B", "LegalEntity"),
)

NUMBER_RE = re.compile(r"^(\d+)\.$")
# Alias lines in the name cell: "a.k.a. X; Y", a bare "a.k.a." heading with
# the aliases following as their own lines, and a whole-line parenthetical
# "(a.k.a. X; Y)".
AKA_RE = re.compile(r"^a\.k\.a\.?:? (.+)$")
AKA_BARE_RE = re.compile(r"^a\.k\.a\.?$")
PAREN_AKA_RE = re.compile(r"^\(a\.k\.a\.?:? (.+)\)$")
# Native-script cells annotate the rendering with its script; the annotation
# labels the group and is not name text.
SPELLING_RE = re.compile(r"^\([A-Za-z]+ spelling\)$")

# Native-script cells whose annotation group wraps one rendering across
# lines; default is one variant rendering per line.
NATIVE_WRAP_PINS = frozenset({("B", "15")})

# Identifying-information labels → CSV column, exactly as printed.
INFO_LABELS = {
    "DOB": "birthDate",
    "POB": "birthPlace",
    "Gender": "gender",
    "Nationality": "nationality",
    "Position(s)": "position",
    "Rank": "position",
    "Address": "address",
    "Address 1": "address",
    "Address 2": "address",
    "Address no. 1": "address",
    "Address no. 2": "address",
    "Address no. 3": "address",
    "Address no. 4": "address",
    "Address no. 5": "address",
    "Location": "address",
    # Mostly cities and street addresses, only rarely a bare country — a
    # place, not a jurisdiction (fleet decision).
    "Place of registration": "address",
    "Principal place of business": "address",
    "Passport number": "passportNumber",
    "ID number": "idNumber",
    "National ID": "idNumber",
    "National ID no.": "idNumber",
    "National ID number": "idNumber",
    "Registration number": "registrationNumber",
    "Business Number": "registrationNumber",
    "Chamber of Commerce Number": "registrationNumber",
    "Date of registration": "incorporationDate",
    "Type of entity": "legalForm",
    "Website": "website",
}
# Columns whose labelled value legitimately continues onto bare follow-on
# lines in this document (post-box lines under an address). Bare lines after
# any other label are new structure.
CONTINUABLE_COLUMNS = frozenset({"address"})
# Relational labels naming other parties: no CSV column, deliberately not
# transcribed. The label line and its bare continuation lines are consumed.
DROP_LABELS = frozenset({"Associated entities", "Associated individuals"})
# Reviewed hand-mappings for identifying-information lines the label rules
# cannot place, keyed by (part, entry) and the exact line. If the source
# line changes, the lookup misses and the run breaks for re-review.
INFO_OVERRIDES: dict[tuple[str, str], dict[str, tuple[tuple[str, str], ...]]] = {
    # A colon-less identifier line after the registration labels.
    ("B", "21"): {
        "Business Registration Number 94186": (("registrationNumber", "94186"),),
    },
}


def verbatim_date(text: str, ctx: str) -> str:
    # Only the dotted form occurs in this document. The printed wording is
    # kept; the recognizer only guards the shape.
    if parse_dotted_date(text) is None:
        raise ParseError(f"{ctx}: unrecognized date {text!r}")
    return text


def check_registry() -> None:
    program = get_program_by_key(PROGRAM_KEY)
    if program is None:
        raise ParseError(f"unknown program key {PROGRAM_KEY!r}")
    if MEASURE not in get_args(Measure):
        raise ParseError(f"invalid measure {MEASURE!r}")
    if MEASURE not in program.measures:
        raise ParseError(f"measure {MEASURE!r} not in {PROGRAM_KEY}")
    for _, _, _, schema_name in PARTS:
        if model.get(schema_name) is None:
            raise ParseError(f"unknown schema {schema_name!r}")


def split_akas(text: str) -> list[str]:
    # Alias lists split on ";" only; a comma- or slash-joined piece stays
    # whole as one value and is categorised in the crawler's review system.
    return [piece.strip() for piece in text.split(";") if piece.strip()]


def parse_name(ctx: str, td: Element, row: Row) -> None:
    lines = cell_lines(td, ctx)
    if not lines:
        raise ParseError(f"{ctx}: empty name cell")
    aliases: list[str] = []
    in_aka_block = False
    for line in lines[1:]:
        if AKA_BARE_RE.match(line) is not None:
            # A bare "a.k.a." heading; the aliases follow as bare lines.
            in_aka_block = True
            continue
        aka = AKA_RE.match(line) or PAREN_AKA_RE.match(line)
        if aka is not None:
            aliases.extend(split_akas(aka.group(1)))
            in_aka_block = True
            continue
        if in_aka_block:
            aliases.extend(split_akas(line))
            continue
        raise ParseError(f"{ctx}: unrecognized name line {line[:60]!r}")
    row.add("name", [lines[0]])
    row.add("alias", aliases)


def parse_native_name(
    ctx: str, part: str, record_id: str, td: Element, row: Row
) -> None:
    # The native-script renderings are aliases; the Latin transliteration
    # column holds the primary name. A "(Farsi spelling)" annotation closes
    # a group of lines: one wrapped rendering for pinned entries, otherwise
    # one variant rendering per line. Cells whose rendering is printed as an
    # embedded image carry only the annotation and yield no alias.
    group: list[str] = []

    def close_group() -> None:
        if not group:
            return
        if (part, record_id) in NATIVE_WRAP_PINS:
            row.add("alias", [" ".join(group)])
        else:
            row.add("alias", group)
        group.clear()

    for line in cell_lines(td, ctx):
        if SPELLING_RE.match(line) is not None:
            close_group()
            continue
        if line.startswith("("):
            raise ParseError(f"{ctx}: unrecognized native-name line {line[:60]!r}")
        group.append(line)
    if group:
        raise ParseError(f"{ctx}: native rendering without script annotation")


def parse_info(ctx: str, part: str, record_id: str, td: Element, row: Row) -> None:
    lines = cell_lines(td, ctx)
    overrides = INFO_OVERRIDES.get((part, record_id), {})
    # A labelled line opens a block; bare lines continue the block for the
    # columns the document has shown continuations for (address post-box
    # lines) or extend dropped relational content. `dropped` marks a block
    # with no column.
    block: str | None = None
    opened_empty = False
    dropped = False
    for line in lines:
        if line in overrides:
            for column, value in overrides[line]:
                row.add(column, [value])
            block, opened_empty, dropped = None, False, False
            continue
        labelled = LABELLED_RE.match(line)
        label = labelled.group(1) if labelled is not None else None
        if label in DROP_LABELS:
            block, opened_empty, dropped = None, False, True
            continue
        if label is not None and label in INFO_LABELS:
            assert labelled is not None
            column = INFO_LABELS[label]
            value = labelled.group(2)
            if value != "":
                row.add(column, split_values(value))
            # An empty-valued label holds its value on the following bare
            # lines, and a value ending in ";" continues its list there.
            block, dropped = column, False
            opened_empty = value == "" or value.endswith(";")
            continue
        if dropped:
            continue
        if block is not None and (opened_empty or block in CONTINUABLE_COLUMNS):
            row.add(block, split_values(line))
            continue
        raise ParseError(f"{ctx}: unrecognized info line {line[:60]!r}")


def continue_row(ctx: str, row: Row, cells: list[Element]) -> None:
    """Merge a continuation row (empty number cell) into the previous entry.

    The one observed continuation carries identifying information only; its
    reasons and date cells are empty.
    """
    ctx = f"{ctx} entry {row.record_id} (cont.)"
    for index in (1, 2):
        if cell_lines(cells[index], ctx):
            raise ParseError(f"{ctx}: unexpected name content in continuation row")
    parse_info(ctx, row.annex.rpartition(".")[2], row.record_id, cells[3], row)
    if cell_lines(cells[4], ctx):
        raise ParseError(f"{ctx}: unexpected reasons in continuation row")
    if cell_lines(cells[5], ctx):
        raise ParseError(f"{ctx}: unexpected date in continuation row")


def parse_row(roman: str, part: str, schema: str, tr: Element) -> Row:
    cells = xpath_elements(tr, "./td|./th")
    ctx = f"{roman}.{part}"
    number = NUMBER_RE.match(cell_line(cells[0], ctx))
    if number is None:
        raise ParseError(f"{ctx}: unrecognized entry number cell")
    record_id = number.group(1)
    ctx = f"{ctx} entry {record_id}"
    row = Row(annex_id(roman, part), schema, MEASURE, record_id=record_id)
    parse_name(ctx, cells[1], row)
    parse_native_name(ctx, part, record_id, cells[2], row)
    parse_info(ctx, part, record_id, cells[3], row)
    row.reason = " ".join(cell_lines(cells[4], ctx))
    row.start_date = verbatim_date(cell_line(cells[5], ctx), ctx)
    return row


def parse_part(roman: str, grid: Element, spec: tuple[str, str, str, str]) -> list[Row]:
    letter, heading, part, schema = spec
    ctx = f"{roman}.{part}"
    col1 = xpath_elements(grid, "./div[contains(@class, 'grid-list-column-1')]")
    if len(col1) != 1 or clean(element_text(col1[0]), ctx) != letter:
        raise ParseError(f"{ctx}: expected part letter {letter!r}")
    col2 = xpath_elements(grid, "./div[contains(@class, 'grid-list-column-2')]")
    if len(col2) != 1:
        raise ParseError(f"{ctx}: expected one part content column")
    rows: list[Row] = []
    seen_heading = False
    for child in col2[0].iterchildren():
        if not isinstance(child.tag, str):
            continue
        cls = child.get("class") or ""
        if child.tag == "p" and cls == "modref":
            check_marker(" ".join(element_text(child).split()), ctx)
            continue
        if child.tag == "p" and cls == "norm":
            if clean(element_text(child), ctx) != heading or seen_heading:
                raise ParseError(f"{ctx}: unexpected part heading")
            seen_heading = True
            continue
        if child.tag == "p" and cls in SKIP_P_CLASSES:
            continue
        if child.tag == "div" and cls == "centered":
            table = xpath_elements(child, ".//table", expect_exactly=1)[0]
            for tr in table_body(ctx, table, PART_HEADER):
                cells = xpath_elements(tr, "./td|./th")
                if not cell_lines(cells[0], ctx):
                    if not rows:
                        raise ParseError(f"{ctx}: continuation before first entry")
                    continue_row(ctx, rows[-1], cells)
                    continue
                rows.append(parse_row(roman, part, schema, tr))
            continue
        raise ParseError(f"{ctx}: unexpected <{child.tag} class={cls!r}>")
    if not seen_heading:
        raise ParseError(f"{ctx}: missing part heading {heading!r}")
    return rows


def parse_annex_iii(roman: str, block: Element) -> list[Row]:
    rows: list[Row] = []
    grids: list[Element] = []
    for child in block.iterchildren():
        if not isinstance(child.tag, str):
            continue
        cls = child.get("class") or ""
        if child.tag == "hr" and cls == "separator-annex":
            continue
        if child.tag == "p" and cls == "modref":
            check_marker(" ".join(element_text(child).split()), roman)
            continue
        if child.tag == "p" and cls in SKIP_P_CLASSES:
            continue
        if child.tag == "div" and "grid-container" in cls:
            grids.append(child)
            continue
        raise ParseError(f"{roman}: unexpected <{child.tag} class={cls!r}>")
    if len(grids) != len(PARTS):
        raise ParseError(f"{roman}: {len(grids)} part blocks, expected {len(PARTS)}")
    for spec, grid in zip(PARTS, grids, strict=True):
        rows.extend(parse_part(roman, grid, spec))
    return rows


def parse_document(doc: Element) -> list[Row]:
    rows: list[Row] = []
    for roman, block in annex_blocks(doc, {TARGET_ANNEX} | NON_TARGET):
        if roman in NON_TARGET:
            continue
        annex_rows = parse_annex_iii(roman, block)
        if not annex_rows:
            raise ParseError(f"{roman}: no entries extracted")
        rows.extend(annex_rows)
    return rows


@click.command(help="Parse consolidated Regulation 2023/1529 into a CSV candidate.")
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
            raise ParseError(f"not a consolidated 2023/1529 CELEX: {celex!r}")
        content = load_source(celex, source)
        doc = html.fromstring(content)
        rows = parse_document(doc)
        records = [to_record(row, FRAMEWORK_CELEX, PROGRAM_KEY) for row in rows]
        validate_records(records)
        csv_path = write_csv(records, FRAMEWORK_CELEX)
        click.echo(json.dumps(summary(records, celex), indent=2))
        click.echo(f"wrote {csv_path}")
    except ParseError as exc:
        raise click.ClickException(str(exc)) from exc


if __name__ == "__main__":
    main()
