"""Parse consolidated Regulation (EU) 2018/1542 (chemical weapons) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation's annexes:

- Annex I — the Article 2 fund-freeze list, with parts A. NATURAL PERSONS
  and B. LEGAL PERSONS, ENTITIES AND BODIES, each printed as one
  four-column table (name, identifying information, grounds for
  designation, date of listing). There is no number column: the entry
  number leads the name cell's first line ("10. Andrei Veniaminovich
  YARIN"); the entries added by amendment M11 print it as bare cell text
  before the paragraph structure instead. Travel bans live in Decision
  (CFSP) 2018/1544, not in this regulation.
- Annex II — competent-authority websites, not designations.

Cyrillic renderings print as parenthetical lines under the Latin name and
become aliases; four early entries print the rendering as the first line
of the identifying-information cell instead (pinned per entry). Long
grounds texts continue into follow-on rows whose name, information and
date cells are empty; the continuation belongs to the preceding entry.
Relational "Associated entity" lines name other parties, have no CSV
column, and are deliberately not transcribed. Dates are transcribed as
the source prints them ("21.1.2019"); the crawler normalizes dates.

Output: data/consolidated/32018R1542.csv (the EU Journal consolidated CSV
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
    annex_id,
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

FRAMEWORK_CELEX = "32018R1542"
PROGRAM_KEY = "EU-CHEM"
# Annex I implements the regulation's Article 2 fund freeze; travel bans
# live in Decision (CFSP) 2018/1544.
MEASURE = "Asset freeze"

NON_TARGET = frozenset({"II"})

SUBTITLE = (
    "LIST OF NATURAL AND LEGAL PERSONS, ENTITIES AND BODIES REFERRED TO IN ARTICLE 2"
)
HEADER = (
    "Name",
    "Identifying information",
    "Grounds for designation",
    "Date of listing",
)
# (part heading, part id, schema) in print order.
PARTS = (
    ("A. NATURAL PERSONS", "A", "Person"),
    ("B. LEGAL PERSONS, ENTITIES AND BODIES", "B", "LegalEntity"),
)

# The entry number leads the name cell's first line; the M11-era entries
# print it as bare cell text before the first paragraph instead.
NUMBER_NAME_RE = re.compile(r"^(\d+)\. (.+)$")
BARE_NUMBER_RE = re.compile(r"^(\d+)\.$")
# Alias lines under the name: "a.k.a. Alexey Andreevich FROLOV".
AKA_RE = re.compile(r"^a\.k\.a\. (.+)$")
# Cyrillic renderings print as fully parenthesized lines under the Latin
# name (or under an a.k.a. line). Unlabeled parentheticals inside the name
# line itself ("… (SSRC)") are part of the printed name and stay in it.
PAREN_NAME_RE = re.compile(r"^\((.+)\)$")

# Four entries print their Cyrillic rendering as the first line of the
# identifying-information cell, twice with an inline a.k.a. label.
NATIVE_INFO_PINS = frozenset({("A", "6"), ("A", "7"), ("A", "8"), ("A", "9")})
NATIVE_INFO_AKA_RE = re.compile(r"^(.+?), a\.k\.a\.: (.+)$")

# This label exceeds LABELLED_RE's length cap and is matched by prefix.
LONG_INN_LABEL = "Taxpayer Personal Identification Number (INN): "

INFO_LABELS = {
    "a.k.a.": "alias",
    "Gender": "gender",
    "Title": "position",
    "Function": "position",
    "Nationality": "nationality",
    "Date of birth": "birthDate",
    "Dates of birth": "birthDate",
    "DOB": "birthDate",
    "Place of birth": "birthPlace",
    "Place of Birth": "birthPlace",
    "Places of Birth": "birthPlace",
    "POB": "birthPlace",
    "Tax-ID No.": "taxNumber",
    "INN": "innCode",
    "Registration number": "registrationNumber",
    "Address": "address",
    "Phone": "phone",
    "Fax": "phone",
    "Web": "website",
    "Website": "website",
    "E-mail": "email",
    "Email": "email",
}
# Relational labels naming other parties: no CSV column, deliberately not
# transcribed.
DROP_LABELS = frozenset({"Associated entity"})


# Only the dotted form occurs in this document.
DATE_FORMATS = ("dotted",)


def split_akas(value: str) -> list[str]:
    # a.k.a. lists separate variants with ";"; a trailing comma is list
    # punctuation. Comma-joined pieces stay whole as one alias value.
    return split_values(value.rstrip(","))


def name_cell(td: Element, ctx: str) -> tuple[str, list[str]]:
    """The name cell's leading bare text (the M11 entry number) and lines."""
    lines = [clean(element_text(p), ctx) for p in xpath_elements(td, ".//p")]
    lines = [line for line in lines if line]
    leading = clean(td.text or "", ctx)
    whole = clean(element_text(td), ctx)
    if " ".join(([leading] if leading else []) + lines) != whole:
        raise ParseError(f"{ctx}: cell text outside <p> structure: {whole[:60]!r}")
    return leading, lines


def parse_name(ctx: str, td: Element, row: Row) -> str:
    """Parse the name cell; returns the entry number."""
    leading, lines = name_cell(td, ctx)
    if not lines:
        raise ParseError(f"{ctx}: empty name cell")
    record_id = ""
    name = lines[0]
    if leading:
        bare = BARE_NUMBER_RE.match(leading)
        if bare is None:
            raise ParseError(f"{ctx}: unrecognized leading cell text {leading!r}")
        record_id = bare.group(1)
    numbered = NUMBER_NAME_RE.match(name)
    if numbered is not None:
        if record_id:
            raise ParseError(f"{ctx}: entry numbered twice")
        record_id, name = numbered.groups()
    row.record_id = record_id
    row.add("name", [name])
    for line in lines[1:]:
        paren = PAREN_NAME_RE.match(line)
        if paren is not None:
            row.add("alias", [paren.group(1)])
            continue
        aka = AKA_RE.match(line)
        if aka is not None:
            row.add("alias", split_akas(aka.group(1)))
            continue
        raise ParseError(f"{ctx}: unrecognized name line {line[:60]!r}")
    return record_id


def parse_info(ctx: str, part: str, record_id: str, td: Element, row: Row) -> None:
    lines = cell_lines(td, ctx)
    # A labelled line opens a block; an empty-valued label ("Address:",
    # "Website:") holds its value on the following bare lines, where a
    # fragment ending in "," wraps onto the next line. A value ending in
    # "," also wraps ("Phone: …-2210758," + "(+963) 11-2224349;").
    block: str | None = None
    opened_empty = False
    dropped = False
    wrapped: str | None = None
    for index, line in enumerate(lines):
        if index == 0 and (part, record_id) in NATIVE_INFO_PINS:
            # The pinned first line is the Cyrillic rendering, optionally
            # with an inline a.k.a. label.
            native_aka = NATIVE_INFO_AKA_RE.match(line)
            if native_aka is not None:
                row.add("alias", [native_aka.group(1)])
                row.add("alias", split_akas(native_aka.group(2)))
            else:
                row.add("alias", [line])
            continue
        if line.startswith(LONG_INN_LABEL):
            row.add("innCode", split_values(line[len(LONG_INN_LABEL) :]))
            block, opened_empty, dropped, wrapped = None, False, False, None
            continue
        labelled = LABELLED_RE.match(line)
        label = labelled.group(1) if labelled is not None else None
        if label in DROP_LABELS:
            block, opened_empty, dropped, wrapped = None, False, True, None
            continue
        if label is not None and label in INFO_LABELS:
            assert labelled is not None
            column = INFO_LABELS[label]
            value = labelled.group(2)
            if column == "alias":
                row.add(column, split_akas(value))
            elif value != "":
                row.add(column, split_values(value))
            block, dropped = column, False
            opened_empty = value == ""
            wrapped = column if value.endswith(",") else None
            continue
        if dropped:
            continue
        if wrapped is not None:
            pieces = split_values(line)
            if len(pieces) != 1:
                raise ParseError(f"{ctx}: unexpected wrap line {line[:60]!r}")
            row.props[wrapped][-1] = f"{row.props[wrapped][-1]} {pieces[0]}"
            wrapped = None
            continue
        if block is not None and opened_empty:
            existing = row.props.get(block)
            if existing and existing[-1].endswith(","):
                existing[-1] = f"{existing[-1]} {line}"
            else:
                row.add(block, split_values(line))
            continue
        raise ParseError(f"{ctx}: unrecognized info line {line[:60]!r}")


def parse_part(roman: str, part: str, schema: str, table: Element) -> list[Row]:
    rows: list[Row] = []
    last_number = 0
    for tr in table_body(f"{roman}.{part}", table, HEADER):
        cells = xpath_elements(tr, "./td|./th")
        ctx = f"{roman}.{part}"
        leading, name_lines = name_cell(cells[0], ctx)
        if not leading and not name_lines:
            # A continuation row: the grounds text of the preceding entry
            # spills over; its other cells are empty.
            if not rows:
                raise ParseError(f"{ctx}: continuation row before first entry")
            if cell_lines(cells[1], ctx) or cell_lines(cells[3], ctx):
                raise ParseError(f"{ctx}: continuation row with new content")
            reason_lines = cell_lines(cells[2], ctx)
            if not reason_lines:
                raise ParseError(f"{ctx}: empty continuation row")
            rows[-1].reason = " ".join([rows[-1].reason, *reason_lines])
            continue
        row = Row(annex_id(roman, part), schema, MEASURE)
        record_id = parse_name(ctx, cells[0], row)
        ctx = f"{ctx} entry {record_id or row.props['name'][0][:30]}"
        if record_id:
            if int(record_id) <= last_number:
                raise ParseError(f"{ctx}: entry number out of order")
            last_number = int(record_id)
        parse_info(ctx, part, record_id, cells[1], row)
        row.reason = " ".join(cell_lines(cells[2], ctx))
        row.start_date = verbatim_date(cell_line(cells[3], ctx), ctx, DATE_FORMATS)
        rows.append(row)
    return rows


def parse_annex_i(roman: str, block: Element) -> list[Row]:
    # The annex prints one subtitle, then each part as a heading followed
    # by one centered four-column table.
    rows: list[Row] = []
    seen_subtitle = False
    part_index = -1
    part_tables = [0 for _ in PARTS]
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
        if child.tag == "p" and cls == "title-gr-seq-level-1":
            if seen_subtitle:
                raise ParseError(f"{roman}: second annex subtitle")
            if clean(element_text(child), roman) != SUBTITLE:
                raise ParseError(f"{roman}: unexpected annex subtitle")
            seen_subtitle = True
            continue
        if child.tag == "p" and cls == "title-gr-seq-level-2":
            part_index += 1
            if part_index >= len(PARTS):
                raise ParseError(f"{roman}: more part headings than parts")
            heading = clean(element_text(child), roman)
            if heading != PARTS[part_index][0]:
                raise ParseError(f"{roman}: unexpected part heading {heading!r}")
            continue
        if child.tag == "div" and cls == "centered":
            if part_index < 0:
                raise ParseError(f"{roman}: table before first part heading")
            _, part, schema = PARTS[part_index]
            part_tables[part_index] += 1
            table = xpath_elements(child, ".//table", expect_exactly=1)[0]
            rows.extend(parse_part(roman, part, schema, table))
            continue
        raise ParseError(f"{roman}: unexpected <{child.tag} class={cls!r}>")
    if not seen_subtitle:
        raise ParseError(f"{roman}: annex subtitle missing")
    if part_tables != [1 for _ in PARTS]:
        raise ParseError(f"{roman}: part table counts {part_tables}, expected one each")
    return rows


def parse_document(doc: Element) -> list[Row]:
    rows: list[Row] = []
    for roman, block in annex_blocks(doc, {"I"} | NON_TARGET):
        if roman in NON_TARGET:
            continue
        annex_rows = parse_annex_i(roman, block)
        if not annex_rows:
            raise ParseError(f"{roman}: no entries extracted")
        rows.extend(annex_rows)
    return rows


@click.command(help="Parse consolidated Regulation 2018/1542 into a CSV candidate.")
@click.argument("celex")
@click.option(
    "--source",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    help="Parse these exact XHTML bytes instead of fetching from CELLAR.",
)
def main(celex: str, source: Path | None) -> None:
    try:
        check_registry(PROGRAM_KEY, [MEASURE], [part[2] for part in PARTS])
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
