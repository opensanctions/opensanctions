"""Parse consolidated Regulation (EU) 1284/2009 (Guinea) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation's annexes:

- Annex II — the Article 6(3) fund-freeze list, one four-column table
  (entry number, name and possible aliases, identifying information,
  reasons) with no date column: the document prints no per-designation
  dates, so `startDate` is empty on every row. There is no part structure;
  each reviewed entry's schema is pinned by its number. Travel bans live
  in Decision 2010/638/CFSP, not in this regulation.
- Annex III — competent-authority websites, not designations.

Annex I (the internal-repression equipment list of the original act) is no
longer present in the consolidated text. Aliases appear as a labelled
"Alias:" second line or as a trailing "(alias …)" parenthetical peeled off
the name; one entry prints the parenthetical mid-name and is kept whole
for the crawler's name review.

Output: data/consolidated/32009R1284.csv (the EU Journal consolidated CSV
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
    write_csv,
)
from lxml import html
from zavod.helpers.html import element_text, xpath_elements
from zavod.util import Element

FRAMEWORK_CELEX = "32009R1284"
PROGRAM_KEY = "EU-GIN"
# Annex II implements the Article 6(3) fund freeze; travel bans live in
# Decision 2010/638/CFSP.
MEASURE = "Asset freeze"

TARGET_ANNEX = "II"
NON_TARGET = frozenset({"III"})

HEADING = (
    "LIST OF NATURAL AND LEGAL PERSONS, ENTITIES OR BODIES REFERRED TO IN ARTICLE 6(3)"
)
HEADER = ("", "Name (and possible aliases)", "Identifying information", "Reasons")

NUMBER_RE = re.compile(r"^(\d+)\.$")
# A trailing "(alias …)" parenthetical on the name line; the printed label
# marks the parenthetical's content as an alias.
NAME_ALIAS_TAIL_RE = re.compile(r"^(.+) \(alias ([^()]+)\)$")

# Annex II mixes natural and legal persons in one table with no part
# structure, so each reviewed entry's schema is pinned here; an entry
# number missing from this table is a new designation to classify.
ENTRY_SCHEMAS = {
    "1": "Person",
    "2": "Person",
    "3": "Person",
    "4": "Person",
    "5": "Person",
}
# Entry 4 prints its alias parenthetical mid-name ("Captain Aboubacar
# Chérif (alias Toumba) DIAKITÉ"); extracting it would assemble the name
# from non-contiguous pieces, so the line is kept whole for the crawler's
# name review.
MIDNAME_ALIAS_PINS = frozenset({"4"})

# Identifying-information labels → CSV column, exactly as printed (entry 2
# prints "DOB." with a stray period inside the label).
INFO_LABELS = {
    "DOB": "birthDate",
    "DOB.": "birthDate",
    "Passport number": "passportNumber",
    "Gender": "gender",
    "Address": "address",
    "Function or profession": "position",
}
# Free-text label whose bare prose value goes to `notes`.
NOTES_LABEL = "Other information"


def parse_name(ctx: str, record_id: str, td: Element, row: Row) -> None:
    lines = cell_lines(td, ctx)
    if not lines:
        raise ParseError(f"{ctx}: empty name cell")
    name = lines[0]
    tail = NAME_ALIAS_TAIL_RE.match(name)
    if tail is not None:
        name = tail.group(1)
        row.add("alias", [tail.group(2)])
    if "(alias" in name and record_id not in MIDNAME_ALIAS_PINS:
        raise ParseError(f"{ctx}: unextracted alias in name {name[:60]!r}")
    row.add("name", [name])
    for line in lines[1:]:
        labelled = LABELLED_RE.match(line)
        if labelled is not None and labelled.group(1) == "Alias":
            row.add("alias", split_values(labelled.group(2)))
            continue
        raise ParseError(f"{ctx}: unrecognized name line {line[:60]!r}")


def parse_info(ctx: str, td: Element, row: Row) -> None:
    for line in cell_lines(td, ctx):
        labelled = LABELLED_RE.match(line)
        if labelled is None:
            raise ParseError(f"{ctx}: unrecognized info line {line[:60]!r}")
        label, value = labelled.group(1), labelled.group(2)
        if value == "":
            raise ParseError(f"{ctx}: label {label!r} without value")
        if label == NOTES_LABEL:
            row.add("notes", [value])
            continue
        if label not in INFO_LABELS:
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
    return row


def parse_annex_ii(annex: str, block: Element) -> list[Row]:
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
        if child.tag == "p" and cls == "title-gr-seq-level-1":
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
        annex_rows = parse_annex_ii(annex, block)
        if not annex_rows:
            raise ParseError(f"{annex}: no entries extracted")
        rows.extend(annex_rows)
    return rows


@click.command(help="Parse consolidated Regulation 1284/2009 into a CSV candidate.")
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
