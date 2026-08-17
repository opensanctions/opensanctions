"""Parse consolidated Regulation (EU) 2017/2063 (Venezuela) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation's annexes:

- Annex I — equipment which might be used for internal repression, not
  designations.
- Annex II — interception equipment, technology and software, not
  designations.
- Annex III — competent-authority websites, not designations.
- Annex IV — the Article 8(3) fund-freeze list, one five-column table
  (entry number, name, identifying information, reasons, date of listing)
  with no part structure. Although the heading admits natural and legal
  persons, every reviewed entry is a natural person and prints a Gender
  line; an entry without one needs schema review, so its absence breaks
  the run. Travel bans live in Decision (CFSP) 2017/2074, not in this
  regulation.
- Annex V — the Article 8(4) list, printed as a heading with no entries;
  the day it gains content the parser breaks and the new shape gets
  reviewed.

Recent entries print an unlabelled leading line stating the person's
function, mapped to position under per-entry pins. Dates are transcribed
as the source prints them ("22.1.2018"); the crawler normalizes dates.

Output: data/consolidated/32017R2063.csv (the EU Journal consolidated CSV
contract, keyed by the framework act). The consolidated version the snapshot
was extracted from is passed as the CELEX argument and pinned in the dataset
YAML's `config.consolidation`, updated in the same commit as the CSV.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import click
from common import (
    LABELLED_RE,
    SKIP_P_CLASSES,
    ParseError,
    Row,
    annex_blocks,
    cell_line,
    cell_lines,
    check_consolidated_celex,
    check_registry,
    clean,
    load_source,
    split_values,
    summary,
    table_body,
    to_record,
    validate_records,
    verbatim_date,
    write_csv,
)
from lxml import html
from zavod.helpers.html import element_text, xpath_elements
from zavod.util import Element

FRAMEWORK_CELEX = "32017R2063"
PROGRAM_KEY = "EU-VEN"
# Annex IV implements the regulation's Article 8(3) fund freeze; travel
# bans live in Decision (CFSP) 2017/2074.
MEASURE = "Asset freeze"

NON_TARGET = frozenset({"I", "II", "III"})
TARGET_ANNEX = "IV"
EMPTY_ANNEX = "V"
EMPTY_ANNEX_SUBTITLE = (
    "List of natural and legal persons, entities and bodies referred to in Article 8(4)"
)

HEADER = (
    "",
    "Name",
    "Identifying information",
    "Reasons",
    "Date of listing",
)

NUMBER_RE = re.compile(r"^(\d+)\.$")
# A labelled alias line under the name ("Alias: José Adelino ORNELLA
# FERREIRA / José Adelino ORNELLAS FERREIRA"); the slash-joined variants
# stay one alias value for the crawler's name review.
ALIAS_LINE_RE = re.compile(r"^Alias: (.+)$")

# Entries whose name wraps across two printed lines (given names / family
# names); default is a single-line name.
WRAPPED_NAME_PINS = frozenset({"17", "18", "48"})
# Entries whose identifying information opens with an unlabelled line
# stating the person's function (the 2025 judiciary/CNE additions).
POSITION_LINE_PINS = frozenset(
    {
        "56",
        "57",
        "58",
        "59",
        "60",
        "61",
        "62",
        "63",
        "64",
        "65",
        "66",
        "67",
        "68",
        "69",
        "70",
    }
)
# One listing date is printed with a stray trailing period ("22.2.2021.");
# the period is list punctuation, not date wording.
DATE_PERIOD_PINS = frozenset({"54"})

# Identifying-information labels → CSV column, exactly as printed.
INFO_LABELS = {
    "Date of birth": "birthDate",
    "Date of Birth": "birthDate",
    "DOB": "birthDate",
    "Place of birth": "birthPlace",
    "POB": "birthPlace",
    "Gender": "gender",
    "Nationality": "nationality",
    "ID number": "idNumber",
    "ID-number": "idNumber",
    "ID Number": "idNumber",
    "ID number (Cédula)": "idNumber",
    "National ID number (Cédula)": "idNumber",
    "Passport number": "passportNumber",
    "Address": "address",
}
# Reviewed hand-mappings for identifying-information lines the label rules
# cannot place, keyed by entry number and the exact line. If the source
# line changes, the lookup misses and the run breaks for re-review.
INFO_OVERRIDES: dict[str, dict[str, tuple[tuple[str, str], ...]]] = {
    # A colon-less identifier line.
    "57": {
        "National ID number (Cédula) 6272864": (("idNumber", "6272864"),),
    },
}


# Only the dotted form occurs in this document.
DATE_FORMATS = ("dotted",)


def parse_name(ctx: str, record_id: str, td: Element, row: Row) -> None:
    lines = cell_lines(td, ctx)
    if not lines:
        raise ParseError(f"{ctx}: empty name cell")
    if record_id in WRAPPED_NAME_PINS:
        if len(lines) != 2:
            raise ParseError(f"{ctx}: wrapped name is not two lines")
        row.add("name", [f"{lines[0]} {lines[1]}"])
        return
    row.add("name", [lines[0]])
    for line in lines[1:]:
        alias = ALIAS_LINE_RE.match(line)
        if alias is not None:
            row.add("alias", split_values(alias.group(1)))
            continue
        raise ParseError(f"{ctx}: unrecognized name line {line[:60]!r}")


def parse_info(ctx: str, record_id: str, td: Element, row: Row) -> None:
    lines = cell_lines(td, ctx)
    overrides = INFO_OVERRIDES.get(record_id, {})
    # An empty-valued label ("ID number (Cédula):") holds its value on the
    # following bare line; other bare lines are new structure.
    block: str | None = None
    opened_empty = False
    for index, line in enumerate(lines):
        if line in overrides:
            for column, value in overrides[line]:
                row.add(column, [value])
            block, opened_empty = None, False
            continue
        labelled = LABELLED_RE.match(line)
        label = labelled.group(1) if labelled is not None else None
        if label is not None and label in INFO_LABELS:
            assert labelled is not None
            column = INFO_LABELS[label]
            value = labelled.group(2)
            if value != "":
                row.add(column, split_values(value))
            block = column
            opened_empty = value == ""
            continue
        if index == 0 and record_id in POSITION_LINE_PINS:
            row.add("position", [line])
            continue
        if block is not None and opened_empty:
            row.add(block, split_values(line))
            continue
        raise ParseError(f"{ctx}: unrecognized info line {line[:60]!r}")


def parse_row(roman: str, tr: Element) -> Row:
    cells = xpath_elements(tr, "./td|./th")
    number = NUMBER_RE.match(cell_line(cells[0], roman))
    if number is None:
        raise ParseError(f"{roman}: unrecognized entry number cell")
    record_id = number.group(1)
    ctx = f"{roman} entry {record_id}"
    row = Row(roman, "Person", MEASURE, record_id=record_id)
    parse_name(ctx, record_id, cells[1], row)
    parse_info(ctx, record_id, cells[2], row)
    # The annex heading admits legal persons, but every reviewed entry is a
    # natural person and prints a Gender line; an entry without one needs
    # schema review.
    if not row.props.get("gender"):
        raise ParseError(f"{ctx}: entry without a gender line")
    row.reason = " ".join(cell_lines(cells[3], ctx))
    date = cell_line(cells[4], ctx)
    if record_id in DATE_PERIOD_PINS:
        date = date.removesuffix(".")
    row.start_date = verbatim_date(date, ctx, DATE_FORMATS)
    return row


def parse_annex_iv(roman: str, block: Element) -> list[Row]:
    rows: list[Row] = []
    tables = 0
    for child in block.iterchildren():
        if not isinstance(child.tag, str):
            continue
        cls = child.get("class") or ""
        if child.tag == "hr" and cls == "separator-annex":
            continue
        if child.tag == "p" and cls in SKIP_P_CLASSES:
            continue
        if child.tag == "div" and cls == "centered":
            tables += 1
            table = xpath_elements(child, ".//table", expect_exactly=1)[0]
            for tr in table_body(roman, table, HEADER):
                rows.append(parse_row(roman, tr))
            continue
        raise ParseError(f"{roman}: unexpected <{child.tag} class={cls!r}>")
    if tables != 1:
        raise ParseError(f"{roman}: {tables} tables, expected one")
    return rows


def check_empty_annex_v(roman: str, block: Element) -> None:
    """The Article 8(4) list prints its heading and no entries; any content
    is new structure that needs review."""
    subtitles = 0
    for child in block.iterchildren():
        if not isinstance(child.tag, str):
            continue
        cls = child.get("class") or ""
        if child.tag == "hr" and cls == "separator-annex":
            continue
        if child.tag == "p" and cls == "":
            if clean(element_text(child), roman) != "":
                raise ParseError(f"{roman}: unexpected paragraph content")
            continue
        if child.tag == "p" and cls in SKIP_P_CLASSES:
            continue
        if child.tag == "p" and cls == "title-gr-seq-level-1":
            subtitles += 1
            if clean(element_text(child), roman) != EMPTY_ANNEX_SUBTITLE:
                raise ParseError(f"{roman}: annex subtitle changed")
            continue
        raise ParseError(f"{roman}: empty annex now has content <{child.tag}>")
    if subtitles != 1:
        raise ParseError(f"{roman}: {subtitles} subtitles, expected one")


def parse_document(doc: Element) -> list[Row]:
    rows: list[Row] = []
    known = {TARGET_ANNEX, EMPTY_ANNEX} | NON_TARGET
    for roman, block in annex_blocks(doc, known):
        if roman in NON_TARGET:
            continue
        if roman == EMPTY_ANNEX:
            check_empty_annex_v(roman, block)
            continue
        annex_rows = parse_annex_iv(roman, block)
        if not annex_rows:
            raise ParseError(f"{roman}: no entries extracted")
        rows.extend(annex_rows)
    return rows


@click.command(help="Parse consolidated Regulation 2017/2063 into a CSV candidate.")
@click.argument("celex")
@click.option(
    "--source",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    help="Parse these exact XHTML bytes instead of fetching from CELLAR.",
)
def main(celex: str, source: Path | None) -> None:
    try:
        check_registry(PROGRAM_KEY, [MEASURE], ["Person"])
        check_consolidated_celex(celex, FRAMEWORK_CELEX)
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
