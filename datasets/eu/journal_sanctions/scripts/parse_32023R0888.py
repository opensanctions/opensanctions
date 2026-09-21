"""Parse consolidated Regulation (EU) 2023/888 (Moldova) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation's annexes:

- Annex I — the Article 2 fund-freeze list, with parts A. Natural persons
  and B. Legal persons, entities and bodies, each printed as one
  five-column table (entry number, name, identifying information,
  statement of reasons, date of listing). The reasons header capitalizes
  differently per part ("Statement of reasons" / "Statement of Reasons").
  Travel bans live in Decision (CFSP) 2023/891, not in this regulation.
- Annex II — competent-authority websites, not designations.

There is no separate native-script name column: Cyrillic and Romanian
renderings print as parenthesized lines under the Latin name (one bare
Cyrillic line is pinned) and become aliases, as do the "a.k.a." lines. A
slash-joined variant pair stays one alias value; splits happen on ";"
only. Some entries print unlabeled leading role prose in the identifying
cell (pinned per entry → position). Dates are transcribed as the source
prints them ("30.5.2023"); the crawler normalizes dates.

Output: data/consolidated/32023R0888.csv (the EU Journal consolidated CSV
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

FRAMEWORK_CELEX = "32023R0888"
PROGRAM_KEY = "EU-MDA"
# Annex I implements the regulation's Article 2 fund freeze; travel bans
# live in Decision (CFSP) 2023/891.
MEASURE = "Asset freeze"

NON_TARGET = frozenset({"II"})

# (part heading, part id, schema, header) in print order; the two parts
# capitalize the reasons column differently.
PARTS = (
    (
        "A. Natural persons",
        "A",
        "Person",
        (
            "",
            "Name",
            "Identifying information",
            "Statement of reasons",
            "Date of listing",
        ),
    ),
    (
        "B. Legal persons, entities and bodies",
        "B",
        "LegalEntity",
        (
            "",
            "Name",
            "Identifying information",
            "Statement of Reasons",
            "Date of listing",
        ),
    ),
)
# Entries whose printed identifiers force a more specific schema: Evrazia
# carries a KPP, a Company-only property.
SCHEMA_PINS = {("B", "2"): "Company"}

NUMBER_RE = re.compile(r"^(\d+)\.$")
# Alias lines in the name cell: "a.k.a. Vladimir ULINICI", including the
# printed typo form "a.ka. Igor Yuryevich CHAYKA" (entry A4).
AKA_RE = re.compile(r"^(?:a\.k\.a\.|a\.ka\.) (.+)$")
# One entry (A1) prints a bare "a.k.a." label line whose alias follows on
# the next line.
BARE_AKA = "a.k.a."
# Native-script and translated renderings print as fully parenthesized
# lines under the Latin name.
PAREN_NAME_RE = re.compile(r"^\((.+)\)$")

# Entries listing an unlabeled variant rendering as a bare line under the
# name; the printed list structure marks it as an alias (B4's Cyrillic
# bloc name prints without parentheses).
ALIAS_LINE_PINS = frozenset({("B", "4")})

# Identifying-information labels → CSV column, exactly as printed.
INFO_LABELS = {
    "DOB": "birthDate",
    "POB": "birthPlace",
    "Gender": "gender",
    "Nationality": "nationality",
    "Function": "position",
    "Passport no": "passportNumber",
    "Passport number": "passportNumber",
    "Passport Number": "passportNumber",
    "ID Card Number": "idNumber",
    "State Identification Number (IDNP)": "idNumber",
    "INN": "innCode",
    "KPP": "kppCode",
    "OGRN": "ogrnCode",
    "Registration number": "registrationNumber",
    "Registration numbers": "registrationNumber",
    "Type of entity": "legalForm",
    "Address": "address",
    "Website": "website",
}
# Labels printed as a header with no value on the line itself; the values
# follow as separately labelled lines (B2's "Registration numbers:" heads
# its OGRN/INN/KPP lines).
EMPTY_VALUE_LABELS = frozenset({"Registration numbers"})
# Columns whose labelled value legitimately continues onto bare follow-on
# lines in this document (a former role printed under the current
# Function, entry A2). Bare lines after any other label are new structure.
CONTINUABLE_COLUMNS = frozenset({"position"})
# Entries opening the identifying cell with unlabeled role prose before
# the first labelled line; each such line is one position value. Any other
# entry printing a leading bare line raises.
LEADING_ROLE_PINS = frozenset({("A", "6"), ("A", "7"), ("A", "10"), ("A", "11")})
# Reviewed hand-mappings for identifying-information lines the label rules
# cannot place, keyed by (part, entry) and the exact line. An empty mapping
# drops the line deliberately.
INFO_OVERRIDES: dict[tuple[str, str], dict[str, tuple[tuple[str, str], ...]]] = {
    # A second passport printed as a bare line under "Passport no:".
    ("A", "2"): {
        "058117566 (Romania)": (("passportNumber", "058117566 (Romania)"),),
    },
}


# Only the dotted form occurs in this document.
DATE_FORMATS = ("dotted",)


def parse_name(ctx: str, part: str, record_id: str, td: Element, row: Row) -> None:
    lines = cell_lines(td, ctx)
    if not lines:
        raise ParseError(f"{ctx}: empty name cell")
    if "a.k" in lines[0]:
        raise ParseError(f"{ctx}: alias label in name line {lines[0][:60]!r}")
    row.add("name", [lines[0]])
    pending_aka = False
    for line in lines[1:]:
        if pending_aka:
            row.add("alias", [line])
            pending_aka = False
            continue
        if line == BARE_AKA:
            pending_aka = True
            continue
        aka = AKA_RE.match(line)
        if aka is not None:
            row.add("alias", split_values(aka.group(1)))
            continue
        paren = PAREN_NAME_RE.match(line)
        if paren is not None:
            row.add("alias", [paren.group(1)])
            continue
        if (part, record_id) in ALIAS_LINE_PINS:
            row.add("alias", [line])
            continue
        raise ParseError(f"{ctx}: unrecognized name line {line[:60]!r}")
    if pending_aka:
        raise ParseError(f"{ctx}: dangling bare a.k.a. label")


def parse_info(ctx: str, part: str, record_id: str, td: Element, row: Row) -> None:
    lines = cell_lines(td, ctx)
    overrides = INFO_OVERRIDES.get((part, record_id), {})
    # A labelled line opens a block; bare lines continue the block for the
    # columns the document has shown continuations for (a former role under
    # the Function). Bare lines before the first label are pinned leading
    # role prose.
    block: str | None = None
    seen_label = False
    for line in lines:
        if line in overrides:
            for column, value in overrides[line]:
                row.add(column, [value])
            block = None
            continue
        labelled = LABELLED_RE.match(line)
        label = labelled.group(1) if labelled is not None else None
        if label is not None and label in INFO_LABELS:
            assert labelled is not None
            column = INFO_LABELS[label]
            value = labelled.group(2)
            if value == "":
                if label not in EMPTY_VALUE_LABELS:
                    raise ParseError(f"{ctx}: label {label!r} without value")
            else:
                row.add(column, split_values(value))
            block, seen_label = column, True
            continue
        if not seen_label and (part, record_id) in LEADING_ROLE_PINS:
            row.add("position", [line])
            continue
        if block in CONTINUABLE_COLUMNS:
            assert block is not None
            row.add(block, split_values(line))
            continue
        raise ParseError(f"{ctx}: unrecognized info line {line[:60]!r}")


def parse_row(
    roman: str, part: str, schema: str, header: tuple[str, ...], tr: Element
) -> Row:
    cells = xpath_elements(tr, "./td|./th")
    ctx = f"{roman}.{part}"
    if len(cells) != len(header):
        raise ParseError(f"{ctx}: row has {len(cells)} cells")
    number = NUMBER_RE.match(cell_line(cells[0], ctx))
    if number is None:
        raise ParseError(f"{ctx}: unrecognized entry number cell")
    record_id = number.group(1)
    ctx = f"{ctx} entry {record_id}"
    schema = SCHEMA_PINS.get((part, record_id), schema)
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
            _, part, schema, header = PARTS[part_index]
            part_tables[part_index] += 1
            table = xpath_elements(child, ".//table", expect_exactly=1)[0]
            for tr in table_body(f"{roman}.{part}", table, header):
                rows.append(parse_row(roman, part, schema, header, tr))
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


@click.command(help="Parse consolidated Regulation 2023/888 into a CSV candidate.")
@click.argument("celex")
@click.option(
    "--source",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    help="Parse these exact XHTML bytes instead of fetching from CELLAR.",
)
def main(celex: str, source: Path | None) -> None:
    try:
        check_registry(
            PROGRAM_KEY,
            [MEASURE],
            [part[2] for part in PARTS] + list(SCHEMA_PINS.values()),
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
