"""Parse consolidated Regulation (EU) 2024/386 (Hamas/PIJ) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation's annexes:

- Annex I — the Article 2 fund-freeze list, with parts A. Natural persons
  and B. Legal persons, groups, entities and bodies, each printed as one
  five-column table (entry number, name, identifying information,
  statement of reasons, date of listing). Travel bans live in Decision
  (CFSP) 2024/385, not in this regulation.
- Annex II — competent-authority websites, not designations.

Name cells print a Latin name, optional fully parenthesized Arabic
renderings (";"-separated within one group, one group wrapping across two
lines under a pin), and alias lists opened by a bare "a.k.a." line whose
following lines each hold one or more ";"-separated variants; part B
prints a labelled "A.k.a.:" list instead. Bare Arabic lines under the name
are native renderings. All of these become aliases; comma-joined variants
inside one piece stay whole for the crawler's name review. Relational
"Owner and chairman" lines name another designee, have no CSV column, and
are deliberately not transcribed. Dates are transcribed as the source
prints them ("19.1.2024"); the crawler normalizes dates.

Output: data/consolidated/32024R0386.csv (the EU Journal consolidated CSV
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

FRAMEWORK_CELEX = "32024R0386"
PROGRAM_KEY = "EU-HAM"
# Annex I implements the regulation's Article 2 fund freeze; travel bans
# live in Decision (CFSP) 2024/385.
MEASURE = "Asset freeze"

NON_TARGET = frozenset({"II"})

HEADER = (
    "",
    "Name",
    "Identifying information",
    "Statement of reasons",
    "Date of listing",
)
# (part heading, part id, schema) in print order.
PARTS = (
    ("A. Natural persons", "A", "Person"),
    ("B. Legal persons, groups, entities and bodies", "B", "LegalEntity"),
)

NUMBER_RE = re.compile(r"^(\d+)\.$")
# A bare "a.k.a." line opens an alias block: each following line holds one
# or more ";"-separated variants. Part B prints a labelled list instead.
AKA_LINE = "a.k.a."
AKA_LABEL = "A.k.a."
# Arabic renderings print as fully parenthesized lines under the Latin
# name; a group holds one or more ";"-separated renderings.
PAREN_NAME_RE = re.compile(r"^\((.+)\)$")
# One parenthesized group wraps across two lines (opening line ends without
# the closing paren); joining is pinned per entry.
WRAPPED_PAREN_PINS = frozenset({("A", "5")})
# Bare non-Latin lines under the name are native renderings (observed as
# Arabic script only).
NATIVE_LINE_RE = re.compile(r"^[^A-Za-z]+$")
# Reviewed hand-mappings for name-cell lines the line rules cannot place,
# keyed by (part, entry) and the exact line. If the source line changes,
# the lookup misses and the run breaks for re-review.
NAME_OVERRIDES: dict[tuple[str, str], dict[str, tuple[tuple[str, str], ...]]] = {
    # The Arabic rendering of the preceding alias prints a stray comma
    # before its closing paren; the comma is list punctuation, not name
    # text.
    ("A", "18"): {"(أبو عمر حسن,)": (("alias", "أبو عمر حسن"),)},
}

# Identifying-information labels → CSV column, exactly as printed.
INFO_LABELS = {
    "DOB": "birthDate",
    "POB": "birthPlace",
    "Nationality": "nationality",
    "Gender": "gender",
    "Function": "position",
    "Passport no": "passportNumber",
    "Passport or ID number": "idNumber",
    "Palestinian National ID no.": "idNumber",
    "Jordanian National ID no.": "idNumber",
    # An entity's territory of activity; no more specific property fits.
    "Active region": "country",
}
# Relational labels naming other parties (part B's "Owner and chairman"
# lines name the part-A designee Hamza): no CSV column, deliberately not
# transcribed.
DROP_LABELS = frozenset({"Owner and chairman"})
# Reviewed hand-mappings for identifying-information lines the label rules
# cannot place, keyed by (part, entry) and the exact line.
INFO_OVERRIDES: dict[tuple[str, str], dict[str, tuple[tuple[str, str], ...]]] = {
    # One entry prints its whole identifying information as a single
    # run-on paragraph (corrigendum ►C1 markup); mapped by hand.
    ("A", "4"): {
        (
            "DOB: 20.9.1967 Nationality: Lebanese Passport or ID number: "
            "3194104 (Lebanon) Gender: male Function: business partner of "
            "the Chouman (Shuman) Group / Shuman for Currency Exchange SARL"
        ): (
            ("birthDate", "20.9.1967"),
            ("nationality", "Lebanese"),
            ("idNumber", "3194104 (Lebanon)"),
            ("gender", "male"),
            (
                "position",
                "business partner of the Chouman (Shuman) Group / "
                "Shuman for Currency Exchange SARL",
            ),
        ),
    },
}


# Only the dotted form occurs in this document.
DATE_FORMATS = ("dotted",)


def add_paren_aliases(content: str, row: Row) -> None:
    row.add("alias", split_values(content))


def parse_name(ctx: str, part: str, record_id: str, td: Element, row: Row) -> None:
    lines = cell_lines(td, ctx)
    if not lines:
        raise ParseError(f"{ctx}: empty name cell")
    row.add("name", [lines[0]])
    overrides = NAME_OVERRIDES.get((part, record_id), {})
    in_akas = False
    wrapped: str | None = None
    for line in lines[1:]:
        if wrapped is not None:
            joined = f"{wrapped} {line}"
            paren = PAREN_NAME_RE.match(joined)
            if paren is None:
                raise ParseError(f"{ctx}: unterminated parenthesized group")
            add_paren_aliases(paren.group(1), row)
            wrapped = None
            continue
        if line in overrides:
            for column, value in overrides[line]:
                row.add(column, [value])
            continue
        if line == AKA_LINE:
            if in_akas:
                raise ParseError(f"{ctx}: second a.k.a. block")
            in_akas = True
            continue
        labelled = LABELLED_RE.match(line)
        if labelled is not None and labelled.group(1) == AKA_LABEL:
            row.add("alias", split_values(labelled.group(2)))
            continue
        paren = PAREN_NAME_RE.match(line)
        if paren is not None:
            add_paren_aliases(paren.group(1), row)
            continue
        if line.startswith("(") and (part, record_id) in WRAPPED_PAREN_PINS:
            wrapped = line
            continue
        if in_akas:
            row.add("alias", split_values(line))
            continue
        if NATIVE_LINE_RE.match(line) is not None:
            row.add("alias", [line])
            continue
        raise ParseError(f"{ctx}: unrecognized name line {line[:60]!r}")
    if wrapped is not None:
        raise ParseError(f"{ctx}: unterminated parenthesized group")


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
    # The annex prints each part as a lettered heading followed by one
    # centered five-column table.
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


@click.command(help="Parse consolidated Regulation 2024/386 into a CSV candidate.")
@click.argument("celex")
@click.option(
    "--source",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    help="Parse these exact XHTML bytes instead of fetching from CELLAR.",
)
def main(celex: str, source: Path | None) -> None:
    try:
        check_registry(
            PROGRAM_KEY, [MEASURE], [schema_name for _, _, schema_name in PARTS]
        )
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
