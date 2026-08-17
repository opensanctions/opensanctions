"""Parse consolidated Regulation (EU) 2023/2147 (Sudan, 2023) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation's annexes:

- Annex I — the Article 2 fund-freeze list, with parts A. Natural Persons
  and B. Entities, each printed as one five-column table (entry number,
  name, identifying information, reasons, date of listing). Travel bans
  live in Decision (CFSP) 2023/2135, not in this regulation.
- Annex II — competent-authority websites, not designations.
- Annexes III and IV — goods lists (CN codes), not designations.

Name cells print alias lists under a standalone "a.k.a." / "a.k.a.:" line,
one alias per line with a ";" terminator on all but the last; one entry
wraps a long alias across lines and is pinned. Some info cells print all
their lines inside a single paragraph separated by <br/>, so cell lines are
extracted br-aware. Relational "Associated …" lines name other parties,
have no CSV column, and are deliberately not transcribed. Dates are
transcribed as the source prints them ("24.6.2024"); the crawler
normalizes dates.

Output: data/consolidated/32023R2147.csv (the EU Journal consolidated CSV
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
    annex_id,
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

FRAMEWORK_CELEX = "32023R2147"
PROGRAM_KEY = "EU-SDNZ"
# Annex I implements the regulation's Article 2 fund freeze; travel bans
# live in Decision (CFSP) 2023/2135.
MEASURE = "Asset freeze"

NON_TARGET = frozenset({"II", "III", "IV"})

SUBTITLE = (
    "List of natural and legal persons, entities or bodies referred to in Article 2"
)
HEADER = (
    "",
    "Name",
    "Identifying information",
    "Reasons",
    "Date of listing",
)
# (part heading, part id, schema) in print order.
PARTS = (
    ("A. Natural Persons", "A", "Person"),
    ("B. Entities", "B", "LegalEntity"),
)

NUMBER_RE = re.compile(r"^(\d+)\.$")
# Alias lists open with a standalone "a.k.a." / "a.k.a.:" line; the label
# also occurs inline with a single alias ("a.k.a. Red Rock Ltd").
AKA_BARE_RE = re.compile(r"^a\.k\.a\.:?$")
AKA_RE = re.compile(r"^a\.k\.a\.:? (.+)$")
# Entry A2's first alias wraps across three lines; within its a.k.a. block,
# a value runs until the printed ";" terminator (the last value has none).
# Everywhere else one printed line is one alias value.
ALIAS_WRAP_PINS = frozenset({("A", "2")})

# Identifying-information labels → CSV column, exactly as printed.
INFO_LABELS = {
    "DOB": "birthDate",
    "POB": "birthPlace",
    "Nationality": "nationality",
    "Gender": "gender",
    "Function": "position",
    "ID number": "idNumber",
    "Passport number": "passportNumber",
    "Passport": "passportNumber",
    "Address": "address",
    "Principal place of business": "address",
    "Type of entity": "legalForm",
    "Date of registration": "incorporationDate",
    "Telephone": "phone",
    "Website": "website",
    "Email": "email",
    "SWIFT/BIC code": "swiftBic",
}
# Columns whose labelled value legitimately continues onto bare follow-on
# lines in this document: further roles under the position, a second
# address line (B4), a second email line (B8). Bare lines after any other
# label are new structure.
CONTINUABLE_COLUMNS = frozenset({"position", "address", "email"})
# Relational labels naming other parties: no CSV column, deliberately not
# transcribed. The label line and its bare continuation lines are consumed.
DROP_LABELS = frozenset(
    {
        "Associated individuals",
        "Associated entities",
        "Associated individuals or entities",
    }
)


# Only the dotted form occurs in this document.
DATE_FORMATS = ("dotted",)


def paragraph_segments(p: Element) -> list[str]:
    """Split one paragraph's text at its <br/> boundaries."""
    segments: list[list[str]] = [[]]

    def walk(el: Element) -> None:
        for child in el.iterchildren():
            if not isinstance(child.tag, str):
                continue
            if child.tag == "br":
                segments.append([])
            else:
                if child.text:
                    segments[-1].append(child.text)
                walk(child)
            if child.tail:
                segments[-1].append(child.tail)

    if p.text:
        segments[0].append(p.text)
    walk(p)
    return ["".join(segment) for segment in segments]


def cell_text_lines(td: Element, ctx: str) -> list[str]:
    """The cell's printed lines: one per <p>, split at <br/> boundaries.

    Most cells print one paragraph per line, but the ▼M6 entries print all
    their info lines inside a single paragraph separated by <br/>.
    """
    lines: list[str] = []
    for p in xpath_elements(td, ".//p"):
        for segment in paragraph_segments(p):
            line = clean(segment, ctx)
            if line:
                lines.append(line)
    whole = clean(element_text(td), ctx)
    if " ".join(lines) != whole:
        raise ParseError(f"{ctx}: cell text outside line structure: {whole[:60]!r}")
    return lines


def scalar_cell(td: Element, ctx: str) -> str:
    lines = cell_text_lines(td, ctx)
    if len(lines) != 1:
        raise ParseError(f"{ctx}: expected one line in cell, got {len(lines)}")
    return lines[0]


def parse_name(ctx: str, part: str, record_id: str, td: Element, row: Row) -> None:
    lines = cell_text_lines(td, ctx)
    if not lines:
        raise ParseError(f"{ctx}: empty name cell")
    row.add("name", [lines[0]])
    in_aka = False
    wrap: list[str] = []
    for line in lines[1:]:
        if AKA_BARE_RE.match(line) is not None:
            if in_aka:
                raise ParseError(f"{ctx}: second a.k.a. block")
            in_aka = True
            continue
        inline = AKA_RE.match(line)
        if inline is not None and not in_aka:
            row.add("alias", [inline.group(1).rstrip(";").strip()])
            continue
        if not in_aka:
            raise ParseError(f"{ctx}: unrecognized name line {line[:60]!r}")
        if (part, record_id) in ALIAS_WRAP_PINS:
            wrap.append(line)
            if line.endswith(";"):
                row.add("alias", [" ".join(wrap).rstrip(";").strip()])
                wrap = []
            continue
        row.add("alias", [line.rstrip(";").strip()])
    if wrap:
        row.add("alias", [" ".join(wrap).rstrip(";").strip()])


def parse_info(ctx: str, td: Element, row: Row) -> None:
    lines = cell_text_lines(td, ctx)
    # A labelled line opens a block; bare lines continue the block for the
    # columns the document has shown continuations for, or extend dropped
    # relational content. A value ending in ";" continues its list on the
    # following bare lines.
    block: str | None = None
    dropped = False
    for line in lines:
        labelled = LABELLED_RE.match(line)
        label = labelled.group(1) if labelled is not None else None
        if label in DROP_LABELS:
            block, dropped = None, True
            continue
        if label is not None and label in INFO_LABELS:
            assert labelled is not None
            column = INFO_LABELS[label]
            value = labelled.group(2)
            if value == "":
                raise ParseError(f"{ctx}: label {label!r} without value")
            row.add(column, split_values(value))
            block, dropped = column, False
            continue
        if dropped:
            continue
        if block in CONTINUABLE_COLUMNS:
            assert block is not None
            row.add(block, split_values(line))
            continue
        raise ParseError(f"{ctx}: unrecognized info line {line[:60]!r}")


def parse_row(roman: str, part: str, schema: str, tr: Element) -> Row:
    cells = xpath_elements(tr, "./td|./th")
    ctx = f"{roman}.{part}"
    number = NUMBER_RE.match(scalar_cell(cells[0], ctx))
    if number is None:
        raise ParseError(f"{ctx}: unrecognized entry number cell")
    record_id = number.group(1)
    ctx = f"{ctx} entry {record_id}"
    row = Row(annex_id(roman, part), schema, MEASURE, record_id=record_id)
    parse_name(ctx, part, record_id, cells[1], row)
    parse_info(ctx, cells[2], row)
    row.reason = " ".join(cell_text_lines(cells[3], ctx))
    row.start_date = verbatim_date(scalar_cell(cells[4], ctx), ctx, DATE_FORMATS)
    return row


def parse_annex_i(roman: str, block: Element) -> list[Row]:
    # The annex prints one subtitle, then each part as a lettered heading
    # followed by one centered five-column table.
    rows: list[Row] = []
    subtitle_seen = False
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
            subtitle = clean(element_text(child), roman)
            if subtitle != SUBTITLE or subtitle_seen:
                raise ParseError(f"{roman}: unexpected subtitle {subtitle!r}")
            subtitle_seen = True
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
            for tr in table_body(f"{roman}.{part}", table, HEADER):
                rows.append(parse_row(roman, part, schema, tr))
            continue
        raise ParseError(f"{roman}: unexpected <{child.tag} class={cls!r}>")
    if not subtitle_seen:
        raise ParseError(f"{roman}: annex subtitle not found")
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


@click.command(help="Parse consolidated Regulation 2023/2147 into a CSV candidate.")
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
