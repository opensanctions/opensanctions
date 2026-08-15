"""Parse consolidated Regulation (EU) 2017/1770 (Mali) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation's annexes:

- Annex Ia — the Article 2b fund-freeze list, printed as one five-column
  table (entry number, name, identifying information, reasons, date of
  listing) with no part structure: natural and legal persons share the
  table, so each reviewed entry's schema is pinned by its number. Travel
  bans live in Decision (CFSP) 2017/1775, not in this regulation. Annex I,
  the former UNSCR 2374 (2017) list, was deleted from the consolidated text
  after the UN regime ended in August 2023.
- Annex II — competent-authority websites, not designations.

Delisted entries leave numbering gaps. One entry prints its native-script
rendering as a second line under the name; the rendering is an alias.
Dates are transcribed as the source prints them ("4.2.2022"); the crawler
normalizes dates.

Output: data/consolidated/32017R1770.csv (the EU Journal consolidated CSV
contract, keyed by the framework act). The consolidated version the snapshot
was extracted from is passed as the CELEX argument and pinned in the dataset
YAML's `consolidation` lookup, updated in the same commit as the CSV.
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
    check_marker,
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

FRAMEWORK_CELEX = "32017R1770"
PROGRAM_KEY = "EU-MLI"
# Annex Ia implements the regulation's Article 2b fund freeze; travel bans
# live in Decision (CFSP) 2017/1775.
MEASURE = "Asset freeze"

TARGET_ANNEX = "Ia"
NON_TARGET = frozenset({"II"})

HEADING = (
    "List of natural or legal persons, entities and bodies referred to in Article 2b:"
)
HEADER = ("", "Name", "Identifying information", "Reasons", "Date of listing")

NUMBER_RE = re.compile(r"^(\d+)\.$")

# Annex Ia mixes natural and legal persons in one table with no part
# structure, so each reviewed entry's schema is pinned here; an entry
# number missing from this table is a new designation to classify.
ENTRY_SCHEMAS = {
    "1": "Person",
    "2": "Person",
    "4": "Person",
    "6": "Person",
}
# Entries printing a native-script rendering as a second line under the
# Latin-script name; the rendering is an alias.
NATIVE_NAME_PINS = frozenset({"6"})

# Identifying-information labels → CSV column, exactly as printed.
INFO_LABELS = {
    "Place of birth": "birthPlace",
    "Date of birth": "birthDate",
    "Nationality": "nationality",
    "Gender": "gender",
    "Passport number": "passportNumber",
    "Position": "position",
    "Function": "position",
    "Address": "address",
}


# Only the dotted form occurs in this document.
DATE_FORMATS = ("dotted",)


def parse_name(ctx: str, record_id: str, td: Element, row: Row) -> None:
    lines = cell_lines(td, ctx)
    if not lines:
        raise ParseError(f"{ctx}: empty name cell")
    row.add("name", [lines[0]])
    for line in lines[1:]:
        if record_id in NATIVE_NAME_PINS:
            row.add("alias", [line])
            continue
        raise ParseError(f"{ctx}: unrecognized name line {line[:60]!r}")


def parse_info(ctx: str, td: Element, row: Row) -> None:
    for line in cell_lines(td, ctx):
        labelled = LABELLED_RE.match(line)
        if labelled is None:
            raise ParseError(f"{ctx}: unrecognized info line {line[:60]!r}")
        label, value = labelled.group(1), labelled.group(2)
        if label not in INFO_LABELS or value == "":
            raise ParseError(f"{ctx}: unrecognized info line {line[:60]!r}")
        row.add(INFO_LABELS[label], split_values(value))


def parse_row(annex: str, tr: Element) -> Row:
    cells = xpath_elements(tr, "./td|./th")
    number = NUMBER_RE.match(cell_line(cells[0], annex))
    if number is None:
        raise ParseError(f"{annex}: unrecognized entry number cell")
    record_id = number.group(1)
    ctx = f"{annex} entry {record_id}"
    schema = ENTRY_SCHEMAS.get(record_id)
    if schema is None:
        raise ParseError(f"{ctx}: entry has no reviewed schema pin")
    row = Row(annex, schema, MEASURE, record_id=record_id)
    parse_name(ctx, record_id, cells[1], row)
    parse_info(ctx, cells[2], row)
    reason_lines = cell_lines(cells[3], ctx)
    if not reason_lines:
        raise ParseError(f"{ctx}: empty reasons cell")
    row.reason = " ".join(reason_lines)
    row.start_date = verbatim_date(cell_line(cells[4], ctx), ctx, DATE_FORMATS)
    return row


def parse_annex_ia(annex: str, block: Element) -> list[Row]:
    rows: list[Row] = []
    seen_heading = False
    for child in block.iterchildren():
        if not isinstance(child.tag, str):
            continue
        cls = child.get("class") or ""
        if child.tag == "hr" and cls == "separator-annex":
            continue
        if child.tag == "p" and cls == "modref":
            check_marker(" ".join(element_text(child).split()), annex)
            continue
        if child.tag == "p" and cls == "norm":
            if clean(element_text(child), annex) != HEADING or seen_heading:
                raise ParseError(f"{annex}: unexpected annex heading")
            seen_heading = True
            continue
        if child.tag == "p" and cls in SKIP_P_CLASSES:
            continue
        if child.tag == "div" and cls == "centered":
            table = xpath_elements(child, ".//table", expect_exactly=1)[0]
            for tr in table_body(annex, table, HEADER):
                rows.append(parse_row(annex, tr))
            continue
        raise ParseError(f"{annex}: unexpected <{child.tag} class={cls!r}>")
    if not seen_heading:
        raise ParseError(f"{annex}: missing annex heading")
    return rows


def parse_document(doc: Element) -> list[Row]:
    rows: list[Row] = []
    for annex, block in annex_blocks(doc, {TARGET_ANNEX} | NON_TARGET):
        if annex in NON_TARGET:
            continue
        annex_rows = parse_annex_ia(annex, block)
        if not annex_rows:
            raise ParseError(f"{annex}: no entries extracted")
        rows.extend(annex_rows)
    return rows


@click.command(help="Parse consolidated Regulation 2017/1770 into a CSV candidate.")
@click.argument("celex")
@click.option(
    "--source",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    help="Parse these exact XHTML bytes instead of fetching from CELLAR.",
)
def main(celex: str, source: Path | None) -> None:
    try:
        check_registry(PROGRAM_KEY, [MEASURE], set(ENTRY_SCHEMAS.values()))
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
