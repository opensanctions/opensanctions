"""Parse consolidated Regulation (EU) 101/2011 (Tunisia) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation's annexes:

- Annex I — the Article 2 fund-freeze list (misappropriation of Tunisian
  state funds), in two printed sections. Section A is one four-column
  table (entry number, Name, Identifying information, Grounds) with no
  date column: the document prints no per-designation dates, so
  `startDate` is empty on every row. Delisted entries leave numbering
  gaps. Section B ("Rights of defence and right to effective judicial
  protection under Tunisian law") opens with annex-level boilerplate and
  then prints one numbered block per section-A entry whose paragraphs
  describe the state of the judicial proceedings; each block's name is
  checked against the section-A entry and its paragraphs go to `notes`.
  Travel measures live in Decision 2011/72/CFSP, not in this regulation.
- Annex II — competent-authority websites, not designations.

The section-A "Other information" value is parentage and marriage prose
("son of …, married to …") mixed with descriptive fragments; relational
mentions inside it are not extracted into columns and the whole value goes
to `notes` untouched. The "Issuing country" line qualifies the identity
card printed above it (always "Tunisia"; the nationality is printed
separately) and, like other identity-document attributes, has no CSV
column — dropped deliberately.

Output: data/consolidated/32011R0101.csv (the EU Journal consolidated CSV
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
    cell_line,
    cell_lines,
    check_marker,
    clean,
    load_source,
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

FRAMEWORK_CELEX = "32011R0101"
CONSOLIDATED_RE = re.compile(r"^02011R0101-\d{8}$")
PROGRAM_KEY = "EU-TUN"
# Annex I implements the Article 2 fund freeze; travel measures live in
# Decision 2011/72/CFSP.
MEASURE = "Asset freeze"

TARGET_ANNEX = "I"
NON_TARGET = frozenset({"II"})

SECTION_A_TITLE = "A. List of persons and entities referred to in Article 2"
SECTION_B_TITLE = (
    "B. Rights of defence and right to effective judicial protection "
    "under Tunisian law:"
)
HEADER = ("", "Name", "Identifying information", "Grounds")

NUMBER_RE = re.compile(r"^(\d+)\.$")

# Section A's heading says "persons and entities" with no part structure,
# so each reviewed entry's schema is pinned by its number; an entry number
# missing from this table is a new designation to classify. The numbering
# gaps are delistings.
ENTRY_SCHEMAS = {
    "1": "Person",
    "2": "Person",
    "3": "Person",
    "5": "Person",
    "6": "Person",
    "7": "Person",
    "8": "Person",
    "9": "Person",
    "10": "Person",
    "11": "Person",
    "12": "Person",
    "13": "Person",
    "15": "Person",
    "16": "Person",
    "17": "Person",
    "20": "Person",
    "25": "Person",
    "30": "Person",
    "31": "Person",
    "32": "Person",
    "33": "Person",
    "34": "Person",
    "35": "Person",
    "40": "Person",
    "42": "Person",
    "46": "Person",
    "48": "Person",
}

# Identifying-information labels → CSV column, exactly as printed.
INFO_LABELS = {
    "POB": "birthPlace",
    "DOB": "birthDate",
    "Nationality": "nationality",
    "ID no": "idNumber",
    "Gender": "gender",
    "Last known address": "address",
}
# Free-text label whose bare prose value goes to `notes`; the parentage
# and marriage mentions inside it stay in the untouched prose.
NOTES_LABEL = "Other information"
# "Issuing country" qualifies the identity card printed above it; identity-
# document attributes have no CSV column and are deliberately dropped.
DROP_LABEL = "Issuing country"

# Two entries print divergent spellings between the designation table and
# their section-B block (15: MEHERZI/MAHERZI; 34: "Ben Raj"/"Ben Haj") —
# source misprints. The reviewed pairs are pinned exactly; the entity name
# is the designation table's spelling.
DEFENCE_NAME_PINS = {
    "15": (
        "Mohamed Montassar Ben Kbaier Ben Mohamed MEHERZI",
        "Mohamed Montassar Ben Kbaier Ben Mohamed MAHERZI",
    ),
    "34": (
        "Najet Bent Haj Hamda Ben Raj Hassen BEN ALI",
        "Najet Bent Haj Hamda Ben Haj Hassen BEN ALI",
    ),
}


def check_registry() -> None:
    program = get_program_by_key(PROGRAM_KEY)
    if program is None:
        raise ParseError(f"unknown program key {PROGRAM_KEY!r}")
    if MEASURE not in get_args(Measure):
        raise ParseError(f"invalid measure {MEASURE!r}")
    if MEASURE not in program.measures:
        raise ParseError(f"measure {MEASURE!r} not in {PROGRAM_KEY}")
    for schema_name in set(ENTRY_SCHEMAS.values()):
        if model.get(schema_name) is None:
            raise ParseError(f"unknown schema {schema_name!r}")


def parse_name(ctx: str, td: Element, row: Row) -> None:
    lines = cell_lines(td, ctx)
    if len(lines) != 1:
        raise ParseError(f"{ctx}: expected a single name line")
    if "(" in lines[0]:
        raise ParseError(f"{ctx}: parenthetical in name {lines[0][:60]!r}")
    row.add("name", [lines[0]])


def parse_info(ctx: str, td: Element, row: Row) -> None:
    for line in cell_lines(td, ctx):
        labelled = LABELLED_RE.match(line)
        if labelled is None:
            raise ParseError(f"{ctx}: unrecognized info line {line[:60]!r}")
        label, value = labelled.group(1), labelled.group(2)
        if value == "":
            raise ParseError(f"{ctx}: label {label!r} without value")
        if label == DROP_LABEL:
            continue
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
    parse_name(ctx, cells[1], row)
    parse_info(ctx, cells[2], row)
    reason_lines = cell_lines(cells[3], ctx)
    if not reason_lines:
        raise ParseError(f"{ctx}: empty grounds cell")
    row.reason = " ".join(reason_lines)
    return row


def parse_defence_block(annex: str, grid: Element, by_id: dict[str, Row]) -> str:
    """Match one section-B block to its entry and add its prose to notes."""
    column_1 = xpath_elements(
        grid, "./div[contains(@class, 'grid-list-column-1')]", expect_exactly=1
    )[0]
    number = NUMBER_RE.match(clean(element_text(column_1), annex))
    if number is None:
        raise ParseError(f"{annex}: unrecognized section-B block label")
    record_id = number.group(1)
    ctx = f"{annex} entry {record_id} (section B)"
    row = by_id.get(record_id)
    if row is None:
        raise ParseError(f"{ctx}: no matching section-A entry")
    column_2 = xpath_elements(
        grid, "./div[contains(@class, 'grid-list-column-2')]", expect_exactly=1
    )[0]
    children = [child for child in column_2 if isinstance(child.tag, str)]
    if not children:
        raise ParseError(f"{ctx}: empty section-B block")
    head, cls = children[0], children[0].get("class") or ""
    if head.tag != "p" or cls != "norm":
        raise ParseError(f"{ctx}: section-B block does not open with a name")
    head_name = clean(element_text(head), ctx)
    if head_name != row.props["name"][0]:
        pin = DEFENCE_NAME_PINS.get(record_id)
        if pin is None or pin != (row.props["name"][0], head_name):
            raise ParseError(f"{ctx}: section-B name differs from the entry name")
    for child in children[1:]:
        cls = child.get("class") or ""
        if child.tag != "p" or cls != "list":
            raise ParseError(f"{ctx}: unexpected <{child.tag} class={cls!r}>")
        row.add("notes", [clean(element_text(child), ctx)])
    return record_id


def parse_annex_i(annex: str, block: Element) -> list[Row]:
    rows: list[Row] = []
    by_id: dict[str, Row] = {}
    # Section state: A holds the designation table; B opens with reviewed
    # boilerplate (list paragraphs and two "—" bullet grids) and then one
    # numbered block per entry.
    section = ""
    defence_ids: list[str] = []
    for child in block.iterchildren():
        if not isinstance(child.tag, str):
            continue
        cls = child.get("class") or ""
        if child.tag == "hr" and cls == "separator-annex":
            continue
        if child.tag == "p" and cls == "modref":
            check_marker(" ".join(element_text(child).split()), annex)
            continue
        if child.tag == "p" and cls in SKIP_P_CLASSES:
            continue
        if child.tag == "p" and cls == "list" and section == "B":
            # Annex-level rights-of-defence boilerplate before the blocks.
            if defence_ids:
                raise ParseError(f"{annex}: prose after section-B blocks")
            continue
        if child.tag == "div" and cls == "":
            title = xpath_elements(child, "./p[@class='norm']", expect_exactly=1)[0]
            text = clean(element_text(title), annex)
            if text == SECTION_A_TITLE and section == "":
                section = "A"
                continue
            if text == SECTION_B_TITLE and section == "A":
                section = "B"
                continue
            raise ParseError(f"{annex}: unexpected section title {text[:60]!r}")
        if child.tag == "div" and cls == "centered" and section == "A":
            table = xpath_elements(child, ".//table", expect_exactly=1)[0]
            for tr in table_body(annex, table, HEADER):
                row = parse_row(annex, tr)
                if row.record_id in by_id:
                    raise ParseError(f"{annex}: duplicate entry {row.record_id}")
                by_id[row.record_id] = row
                rows.append(row)
            continue
        if child.tag == "div" and "grid-container" in cls and section == "B":
            column_1 = xpath_elements(
                child, "./div[contains(@class, 'grid-list-column-1')]", expect_exactly=1
            )[0]
            if clean(element_text(column_1), annex) == "—":
                # The two boilerplate bullet grids; their content is
                # annex-level prose, not entity data.
                if defence_ids:
                    raise ParseError(f"{annex}: bullet grid after section-B blocks")
                continue
            defence_ids.append(parse_defence_block(annex, child, by_id))
            continue
        raise ParseError(f"{annex}: unexpected <{child.tag} class={cls!r}>")
    if section != "B":
        raise ParseError(f"{annex}: section structure incomplete")
    if sorted(defence_ids) != sorted(by_id):
        raise ParseError(f"{annex}: section-B blocks do not match the entries")
    return rows


def parse_document(doc: Element) -> list[Row]:
    rows: list[Row] = []
    for annex, block in annex_blocks(doc, {TARGET_ANNEX} | NON_TARGET):
        if annex in NON_TARGET:
            continue
        annex_rows = parse_annex_i(annex, block)
        if not annex_rows:
            raise ParseError(f"{annex}: no entries extracted")
        rows.extend(annex_rows)
    return rows


@click.command(help="Parse consolidated Regulation 101/2011 into a CSV candidate.")
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
            raise ParseError(f"not a consolidated 101/2011 CELEX: {celex!r}")
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
