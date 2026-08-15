"""Parse consolidated Regulation (EU) 2019/1716 (Nicaragua) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation's annexes:

- Annex I — the Article 2 fund-freeze list, with parts A. Natural persons
  and B. Legal persons, entities and bodies, each printed as one
  five-column table (entry number, name, identifying information, reasons,
  date of listing). Travel bans live in Decision (CFSP) 2019/1720, not in
  this regulation.
- Annex II — competent-authority websites, not designations.

Some entries print a second name line: a labelled "Alias: …" variant, an
unlabeled Spanish rendering of an institution name (an alias by the printed
list structure), or a wrap of the name itself. Passport validity lines
("Issued: …", "Expires: …") qualify the passport printed above them, have
no CSV column, and are deliberately not transcribed. Dates are transcribed
as the source prints them ("4.5.2020"); the crawler normalizes dates.

Output: data/consolidated/32019R1716.csv (the EU Journal consolidated CSV
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

FRAMEWORK_CELEX = "32019R1716"
PROGRAM_KEY = "EU-NIC"
# Annex I implements the regulation's Article 2 fund freeze; travel bans
# live in Decision (CFSP) 2019/1720.
MEASURE = "Asset freeze"

NON_TARGET = frozenset({"II"})

HEADER = (
    "",
    "Name",
    "Identifying information",
    "Reasons",
    "Date of listing",
)
# (part heading, part id, schema) in print order.
PARTS = (
    ("A. Natural persons referred to in Article 2", "A", "Person"),
    (
        "B. Legal persons, entities and bodies referred to in Article 2",
        "B",
        "LegalEntity",
    ),
)

NUMBER_RE = re.compile(r"^(\d+)\.$")
# A labelled variant-name line under the name ("Alias: Rosario María
# MURILLO DE ORTEGA").
ALIAS_RE = re.compile(r"^Alias: (.+)$")

# One name wraps onto a second line (A12 "Alba Luz RAMOS" / "VANEGAS"); the
# continuation joins the name.
WRAPPED_NAME_PINS = frozenset({("A", "12")})
# Institution entries listing their unlabeled Spanish rendering as a bare
# line under the name; the printed list structure marks it as an alias.
ALIAS_LINE_PINS = frozenset({("B", "1"), ("B", "2")})

# Identifying-information labels → CSV column, exactly as printed.
INFO_LABELS = {
    "Date of birth": "birthDate",
    "Place of birth": "birthPlace",
    "Gender": "gender",
    "Nationality": "nationality",
    "Position": "position",
    "Position(s)": "position",
    "Rank": "position",
    "Passport number": "passportNumber",
    "ID number": "idNumber",
    "Address": "address",
    "Headquarters": "address",
    "Date of establishment": "incorporationDate",
    "Date of registration": "incorporationDate",
    "Website": "website",
    "Email": "email",
}
# Labels with no CSV column, deliberately not transcribed: passport
# validity dates qualifying the passport printed above them.
DROP_LABELS = frozenset({"Issued", "Expires"})
# Reviewed hand-mappings for identifying-information lines the label rules
# cannot place, keyed by (part, entry) and the exact line. If the source
# line changes, the lookup misses and the run breaks for re-review.
INFO_OVERRIDES: dict[tuple[str, str], dict[str, tuple[tuple[str, str], ...]]] = {
    # Unlabeled leading lines stating the entry's function.
    ("A", "16"): {
        "Son of Daniel Ortega and Rosario Murillo, Advisor to the Presidency": (
            (
                "position",
                "Son of Daniel Ortega and Rosario Murillo, Advisor to the Presidency",
            ),
        ),
    },
    ("A", "20"): {
        (
            "Director of the Nicaraguan Institute of Telecommunications and "
            "Postal Services, daughter of the General Director of the "
            "Nicaraguan National Police Francisco Javier Díaz Madriz"
        ): (
            (
                "position",
                "Director of the Nicaraguan Institute of Telecommunications "
                "and Postal Services, daughter of the General Director of "
                "the Nicaraguan National Police Francisco Javier Díaz Madriz",
            ),
        ),
    },
    ("A", "21"): {
        (
            "Superintendent of the Superintendency of Banks and other "
            "Financial Institutions of Nicaragua"
        ): (
            (
                "position",
                "Superintendent of the Superintendency of Banks and other "
                "Financial Institutions of Nicaragua",
            ),
        ),
    },
}


# Only the dotted form occurs in this document.
DATE_FORMATS = ("dotted",)


def parse_name(ctx: str, part: str, record_id: str, td: Element, row: Row) -> None:
    lines = cell_lines(td, ctx)
    if not lines:
        raise ParseError(f"{ctx}: empty name cell")
    name = lines[0]
    for line in lines[1:]:
        aka = ALIAS_RE.match(line)
        if aka is not None:
            row.add("alias", split_values(aka.group(1)))
            continue
        if (part, record_id) in WRAPPED_NAME_PINS:
            name = f"{name} {line}"
            continue
        if (part, record_id) in ALIAS_LINE_PINS:
            row.add("alias", [line])
            continue
        raise ParseError(f"{ctx}: unrecognized name line {line[:60]!r}")
    row.add("name", [name])


def parse_info(ctx: str, part: str, record_id: str, td: Element, row: Row) -> None:
    overrides = INFO_OVERRIDES.get((part, record_id), {})
    for line in cell_lines(td, ctx):
        if line in overrides:
            for column, value in overrides[line]:
                row.add(column, [value])
            continue
        labelled = LABELLED_RE.match(line)
        label = labelled.group(1) if labelled is not None else None
        if label in DROP_LABELS:
            continue
        if label is not None and label in INFO_LABELS:
            assert labelled is not None
            value = labelled.group(2)
            if value == "":
                raise ParseError(f"{ctx}: label {label!r} without value")
            row.add(INFO_LABELS[label], split_values(value))
            continue
        raise ParseError(f"{ctx}: unrecognized info line {line[:60]!r}")


def parse_row(roman: str, part: str, schema: str, tr: Element) -> Row:
    cells = xpath_elements(tr, "./td|./th")
    ctx = f"{roman}.{part}"
    number = NUMBER_RE.match(cell_line(cells[0], ctx))
    if number is None:
        raise ParseError(f"{ctx}: unrecognized entry number cell")
    record_id = number.group(1)
    ctx = f"{ctx} entry {record_id}"
    row = Row(annex_id(roman, part), schema, MEASURE, record_id=record_id)
    parse_name(ctx, part, record_id, cells[1], row)
    parse_info(ctx, part, record_id, cells[2], row)
    row.reason = " ".join(cell_lines(cells[3], ctx))
    row.start_date = verbatim_date(cell_line(cells[4], ctx), ctx, DATE_FORMATS)
    return row


def parse_annex_i(roman: str, block: Element) -> list[Row]:
    # The annex prints each part as a heading followed by one centered
    # five-column table.
    rows: list[Row] = []
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


@click.command(help="Parse consolidated Regulation 2019/1716 into a CSV candidate.")
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
