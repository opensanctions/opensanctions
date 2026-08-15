"""Parse consolidated Regulation (EU) 208/2014 (Ukraine misappropriation) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation's annexes:

- Annex I — the Article 2 fund-freeze list (misappropriation of Ukrainian
  state funds, the Yanukovych-era officials), in two printed sections.
  Section A is one five-column table (entry number, Name, Identifying
  information, Statement of reasons, Date of listing); delistings and
  court annulments have shrunk it to three entries, leaving numbering
  gaps. Section B ("Rights of defence and right to effective judicial
  protection") opens with annex-level boilerplate about the Ukrainian
  Code of Criminal Procedure and then prints one numbered heading per
  section-A entry whose paragraphs describe the state of the judicial
  proceedings; each heading's name is checked against the section-A
  entry and its paragraphs go to `notes`. Travel measures live in
  Decision 2014/119/CFSP, not in this regulation.
- Annex II — competent-authority websites, not designations.

The identifying-information cell is one unlabelled prose sentence per
entry; each reviewed sentence is transcribed in an exact-string mapping
below that extracts only contiguous printed spans (birth data, position).
Name cells print Ukrainian and Russian renderings as parenthesized lines
or line pairs under the Latin name; each rendering is one alias.

Output: data/consolidated/32014R0208.csv (the EU Journal consolidated CSV
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

FRAMEWORK_CELEX = "32014R0208"
PROGRAM_KEY = "EU-UKR"
# Annex I implements the Article 2 fund freeze; travel measures live in
# Decision 2014/119/CFSP.
MEASURE = "Asset freeze"

TARGET_ANNEX = "I"
NON_TARGET = frozenset({"II"})

SECTION_A_TITLE = (
    "A. List of natural and legal persons, entities and bodies referred to in Article 2"
)
SECTION_B_TITLE = "B. Rights of defence and right to effective judicial protection"
# Section B's two sub-headings: annex-level boilerplate about the Code of
# Criminal Procedure, then the per-entry blocks.
SECTION_B_LAW_TITLE = (
    "The rights of defence and the right to effective judicial protection "
    "under the Code of Criminal Procedure of Ukraine"
)
SECTION_B_ENTRIES_TITLE = (
    "Application of the rights of defence and the right to effective "
    "judicial protection of each of the listed persons"
)
HEADER = (
    "",
    "Name",
    "Identifying information",
    "Statement of reasons",
    "Date of listing",
)

NUMBER_RE = re.compile(r"^(\d+)\.$")
DEFENCE_HEADING_RE = re.compile(r"^(\d+)\.\s+(.+)$")
# A name line ending in a parenthesized native rendering ("… Kurchenko
# (Сергiй Вiталiйович Курченко)"); the trailing group is one alias.
NAME_PAREN_TAIL_RE = re.compile(r"^(.+?) \(([^()]+)\)$")

# Section A's heading admits legal persons but every reviewed entry is a
# natural person, so each entry's schema is pinned by its number; an entry
# number missing from this table is a new designation to classify. The
# numbering gaps are delistings.
ENTRY_SCHEMAS = {
    "2": "Person",
    "6": "Person",
    "12": "Person",
}

# The identifying-information cell is one unlabelled prose sentence. Each
# reviewed sentence is pinned exactly and mapped to columns holding only
# contiguous printed spans; a changed or new sentence breaks for re-review.
INFO_SENTENCES: dict[str, tuple[str, tuple[tuple[str, str], ...]]] = {
    "2": (
        "born on 20 January 1963 in Kostiantynivka (Donetsk oblast), "
        "former Minister of Internal Affairs.",
        (
            ("birthDate", "20 January 1963"),
            ("birthPlace", "Kostiantynivka (Donetsk oblast)"),
            ("position", "former Minister of Internal Affairs"),
        ),
    ),
    "6": (
        "born on 16 October 1959, former Deputy Minister of Internal Affairs",
        (
            ("birthDate", "16 October 1959"),
            ("position", "former Deputy Minister of Internal Affairs"),
        ),
    ),
    "12": (
        "Born on 21 September 1985 in Kharkiv, businessman",
        (
            ("birthDate", "21 September 1985"),
            ("birthPlace", "Kharkiv"),
            ("position", "businessman"),
        ),
    ),
}


# Only the dotted form occurs in this document.
DATE_FORMATS = ("dotted",)


def parse_name(ctx: str, td: Element, row: Row) -> None:
    lines = cell_lines(td, ctx)
    if not lines:
        raise ParseError(f"{ctx}: empty name cell")
    first = lines[0]
    tail = NAME_PAREN_TAIL_RE.match(first)
    if tail is not None:
        row.add("name", [tail.group(1)])
        row.add("alias", [tail.group(2)])
    elif "(" in first:
        raise ParseError(f"{ctx}: mid-name parenthetical {first[:60]!r}")
    else:
        row.add("name", [first])
    # Further lines are printed renderings: fully-parenthesized Cyrillic
    # lines (a trailing comma separates renderings) or a bare Latin
    # transliteration line — one alias each.
    for line in lines[1:]:
        value = line.removesuffix(",")
        if value.startswith("(") and value.endswith(")"):
            value = value[1:-1].strip()
            if "(" in value or ")" in value:
                raise ParseError(f"{ctx}: nested parens in rendering {line[:60]!r}")
        elif "(" in value or ")" in value:
            raise ParseError(f"{ctx}: unrecognized name line {line[:60]!r}")
        if value == "":
            raise ParseError(f"{ctx}: empty name rendering")
        row.add("alias", [value])


def parse_info(ctx: str, record_id: str, td: Element, row: Row) -> None:
    lines = cell_lines(td, ctx)
    if len(lines) != 1:
        raise ParseError(f"{ctx}: expected a single info sentence")
    reviewed = INFO_SENTENCES.get(record_id)
    if reviewed is None or reviewed[0] != lines[0]:
        raise ParseError(f"{ctx}: unreviewed info sentence {lines[0][:60]!r}")
    for column, value in reviewed[1]:
        row.add(column, [value])


def parse_row(annex: str, tr: Element) -> Row:
    cells = xpath_elements(tr, "./td|./th")
    if len(cells) != 5:
        raise ParseError(f"{annex}: expected five cells in a data row")
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
    parse_info(ctx, record_id, cells[2], row)
    reason_lines = cell_lines(cells[3], ctx)
    if not reason_lines:
        raise ParseError(f"{ctx}: empty reasons cell")
    row.reason = " ".join(reason_lines)
    row.start_date = verbatim_date(cell_line(cells[4], ctx), ctx, DATE_FORMATS)
    return row


def parse_annex_i(annex: str, block: Element) -> list[Row]:
    rows: list[Row] = []
    by_id: dict[str, Row] = {}
    # Walk state: "" → before section A; "A" → the designation table;
    # "B-law" → the Code of Criminal Procedure boilerplate; "B-entries" →
    # the per-entry blocks.
    section = ""
    current: Row | None = None
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
        if child.tag == "div" and cls == "centered" and section == "A":
            table = xpath_elements(child, ".//table", expect_exactly=1)[0]
            for tr in table_body(annex, table, HEADER):
                row = parse_row(annex, tr)
                if row.record_id in by_id:
                    raise ParseError(f"{annex}: duplicate entry {row.record_id}")
                by_id[row.record_id] = row
                rows.append(row)
            continue
        text = clean(element_text(child), annex)
        if child.tag == "p" and cls == "title-gr-seq-level-1":
            if text == SECTION_A_TITLE and section == "":
                section = "A"
                continue
            if text == SECTION_B_TITLE and section == "A":
                section = "B"
                continue
            raise ParseError(f"{annex}: unexpected section title {text[:60]!r}")
        if child.tag == "p" and cls == "title-gr-seq-level-2" and section == "B":
            if text == SECTION_B_LAW_TITLE:
                section = "B-law"
                continue
            raise ParseError(f"{annex}: unexpected sub-heading {text[:60]!r}")
        if child.tag == "p" and cls == "title-gr-seq-level-2" and section == "B-law":
            if text == SECTION_B_ENTRIES_TITLE:
                section = "B-entries"
                continue
            raise ParseError(f"{annex}: unexpected sub-heading {text[:60]!r}")
        if child.tag == "p" and cls == "norm" and section == "B-law":
            # Annex-level boilerplate about Ukrainian procedural law, not
            # entity data — deliberately not transcribed.
            continue
        if (
            child.tag == "p"
            and cls == "title-gr-seq-level-3"
            and section == "B-entries"
        ):
            heading = DEFENCE_HEADING_RE.match(text)
            if heading is None:
                raise ParseError(f"{annex}: unrecognized block heading {text[:60]!r}")
            record_id, head_name = heading.groups()
            ctx = f"{annex} entry {record_id} (section B)"
            current = by_id.get(record_id)
            if current is None:
                raise ParseError(f"{ctx}: no matching section-A entry")
            if head_name != current.props["name"][0]:
                raise ParseError(f"{ctx}: block name differs from the entry name")
            defence_ids.append(record_id)
            continue
        if child.tag == "p" and cls == "norm" and section == "B-entries":
            if current is None:
                raise ParseError(f"{annex}: prose before the first block heading")
            current.add("notes", [text])
            continue
        raise ParseError(f"{annex}: unexpected <{child.tag} class={cls!r}>")
    if section != "B-entries":
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


@click.command(help="Parse consolidated Regulation 208/2014 into a CSV candidate.")
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
