"""Parse consolidated Regulation (EU) 2015/735 (South Sudan) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation's target annexes:

- Annex I — persons designated under the UN South Sudan sanctions regime
  (Article 5(1)), in the UN paragraph layout: sections A. PERSONS (eight
  numbered entries, each a name heading with an "(alias: …)" parenthetical,
  labelled field lines, then the narrative-summary section, which is the
  reason for listing) and B. LEGAL PERSONS, ENTITIES AND BODIES, which
  prints no entries — no entity has ever been listed, and no entry format
  is taught, so a first listing breaks for review.
- Annex II — the EU-autonomous Article 5(2) list, one five-column table
  ("", Name, Identifying information, Statement of Reasons, Date of
  listing) with a single reviewed entry.

Annex III lists competent-authority websites, not designations. Travel
bans live in Decision (CFSP) 2015/740. Dates are transcribed as the source
prints them ("1 Jul. 2015", "13 July 2018", "3.2.2018"); the crawler
normalizes dates. The alias parentheticals' closing paren is inconsistently
printed (missing on entries 2-6); an alias item left with more closing than
opening parens sheds one trailing paren, and list commas before the next
lettered marker are stripped.

Output: data/consolidated/32015R0735.csv (the EU Journal consolidated CSV
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
    annex_id,
    cell_line,
    cell_lines,
    check_marker,
    clean,
    load_source,
    parse_abbrev_date,
    parse_dotted_date,
    parse_worded_date,
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

FRAMEWORK_CELEX = "32015R0735"
CONSOLIDATED_RE = re.compile(r"^02015R0735-\d{8}$")
PROGRAM_KEY = "EU-SSD"
# Annexes I and II implement the Article 5 fund freeze; travel bans live in
# Decision (CFSP) 2015/740.
MEASURE = "Asset freeze"

NON_TARGET = frozenset({"III"})

I_SUBTITLE = (
    "LIST OF NATURAL AND LEGAL PERSONS, ENTITIES AND BODIES REFERRED TO IN ARTICLE 5(1)"
)
# Section headings of Annex I in print order. Only A. PERSONS has entries;
# section B has never listed anyone and no entry format is taught for it.
I_PARTS = ("A. PERSONS", "B. LEGAL PERSONS, ENTITIES AND BODIES")
I_SCHEMA = "Person"

# Entry headings: "1. Gabriel JOK RIAK MAKOL (alias: a) Gabriel Jok b) …)".
# The label prints as "alias:", "Alias:" and once "alias a.k.a.:"; the
# closing paren of the group is often missing.
HEADING_RE = re.compile(r"^(\d+)\. (.+?) \((?:alias a\.k\.a\.|[Aa]lias): (.+)$")

# Labelled field lines → CSV column, with the observed casing variants.
FIELD_COLUMNS = {
    "Title": "position",
    "Designation": "position",
    "Date of Birth": "birthDate",
    "Date of birth": "birthDate",
    "Place of Birth": "birthPlace",
    "Place of birth": "birthPlace",
    "Nationality": "nationality",
    "Passport no": "passportNumber",
    "National identification no": "idNumber",
    "Address": "address",
}
DATE_LABEL = "Date of UN designation"
NOTES_LABEL = "Other information"
LABELLED_LINE_RE = re.compile(r"^([A-Za-z][A-Za-z .]{2,40}?):\s*(.*)$")

# Everything after this line is the source's reason for listing.
NARRATIVE_SENTINEL = (
    "Information from the narrative summary of reasons for listing "
    "provided by the Sanctions Committee:"
)
# Content-free sub-heading inside the narrative.
ADDITIONAL_INFO = frozenset({"Additional information", "Additional information:"})
# Entry 6 prints its narrative without the sentinel line; for this pinned
# entry only, the first unlabelled paragraph after Other information starts
# the reason.
NO_SENTINEL_PINS = frozenset({("A", "6")})

# Reviewed hand-mappings for field lines that glue two labelled facts into
# one printed line, keyed by (part, entry) and the exact line. If the source
# line changes, the lookup misses and the run breaks for re-review.
INFO_OVERRIDES: dict[tuple[str, str], dict[str, tuple[tuple[str, str], ...]]] = {
    ("A", "3"): {
        "Nationality: South Sudan Passport no: R00012098, South Sudan": (
            ("nationality", "South Sudan"),
            ("passportNumber", "R00012098, South Sudan"),
        ),
    },
    ("A", "5"): {
        "Nationality: South Sudan, Passport no: R00005943, South Sudan": (
            ("nationality", "South Sudan"),
            ("passportNumber", "R00005943, South Sudan"),
        ),
    },
}

II_SUBTITLE = (
    "List of natural and legal persons, entities and bodies referred to in Article 5(2)"
)
II_HEADER = (
    "",
    "Name",
    "Identifying information",
    "Statement of Reasons",
    "Date of listing",
)
II_INFO_LABELS = {
    "DOB": "birthDate",
    "POB": "birthPlace",
    "Gender": "gender",
}
# The Annex II heading admits natural and legal persons; each reviewed
# entry's schema is pinned by number, and a new number breaks the run for
# classification.
II_ENTRY_SCHEMAS = {"1": "Person"}
NUMBER_RE = re.compile(r"^(\d+)\.$")


def verbatim_date(text: str, ctx: str) -> str:
    # Formats observed in this document: UN abbreviated ("1 Jul. 2015") and
    # worded ("13 July 2018") dates in Annex I, dotted ("3.2.2018") in
    # Annex II. The printed wording is kept; the recognizers only guard the
    # shape.
    for parse in (parse_abbrev_date, parse_worded_date, parse_dotted_date):
        if parse(text) is not None:
            return text
    raise ParseError(f"{ctx}: unrecognized date {text!r}")


def split_plain_lettered(ctx: str, value: str) -> list[str]:
    """Split a UN "a) … b) …" list at depth-zero, in-sequence letter markers."""
    starts = [0]
    expected = "b"
    depth = 0
    i = 3
    while i < len(value) - 2:
        char = value[i]
        if (
            depth == 0
            and value[i - 1] == " "
            and char == expected
            and value[i + 1 : i + 3] == ") "
        ):
            starts.append(i)
            expected = chr(ord(expected) + 1)
            i += 3
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth = max(0, depth - 1)
        i += 1
    items: list[str] = []
    for index, start in enumerate(starts):
        end = starts[index + 1] if index + 1 < len(starts) else len(value)
        # A list comma or semicolon before the next marker is punctuation,
        # not value ("a) … Commander; b) …").
        items.append(value[start + 3 : end].strip().rstrip(",;").strip())
    return items


def field_values(ctx: str, value: str) -> list[str]:
    """Expand one field into CSV values ("a) …" lists split, else whole)."""
    if value.startswith("a) "):
        return split_plain_lettered(ctx, value)
    return [value]


def split_aliases(ctx: str, blob: str) -> list[str]:
    """Split the heading's alias parenthetical into alias values.

    The group's closing paren is inconsistently printed (present on entries
    1, 7 and 8, missing on 2-6); an item left with more closing than opening
    parens sheds one trailing paren.
    """
    items = []
    for item in field_values(ctx, blob):
        if item.endswith(")") and item.count(")") > item.count("("):
            item = item[:-1].strip()
        if not item:
            raise ParseError(f"{ctx}: empty alias item in {blob[:60]!r}")
        items.append(item)
    return items


def parse_heading(ctx: str, part: str, text: str) -> Row:
    match = HEADING_RE.match(text)
    if match is None:
        raise ParseError(f"{ctx}: unrecognized entry heading {text[:60]!r}")
    record_id = match.group(1)
    entry_ctx = f"{ctx} entry {record_id}"
    row = Row(annex_id("I", part), I_SCHEMA, MEASURE, record_id=record_id)
    row.add("name", [match.group(2)])
    row.add("alias", split_aliases(entry_ctx, match.group(3)))
    return row


def parse_fields(ctx: str, part: str, row: Row, lines: list[str]) -> None:
    overrides = INFO_OVERRIDES.get((part, row.record_id), {})
    in_notes = False
    in_reason = False
    reason_parts: list[str] = []
    for line in lines:
        if line == NARRATIVE_SENTINEL:
            if in_reason:
                raise ParseError(f"{ctx}: repeated narrative sentinel")
            in_reason = True
            continue
        if in_reason:
            if line in ADDITIONAL_INFO:
                continue
            reason_parts.append(line)
            continue
        if line in overrides:
            for column, value in overrides[line]:
                row.add(column, [value])
            continue
        labelled = LABELLED_LINE_RE.match(line)
        label = labelled.group(1) if labelled is not None else None
        if label == DATE_LABEL:
            value = labelled.group(2) if labelled is not None else ""
            row.start_date = verbatim_date(value, ctx)
            continue
        if label == NOTES_LABEL:
            value = labelled.group(2) if labelled is not None else ""
            if not value:
                raise ParseError(f"{ctx}: empty Other information line")
            row.add("notes", [value])
            in_notes = True
            continue
        if label in FIELD_COLUMNS:
            value = labelled.group(2) if labelled is not None else ""
            if not value:
                raise ParseError(f"{ctx}: empty value for label {label!r}")
            row.add(FIELD_COLUMNS[label], field_values(ctx, value))
            continue
        if in_notes and not in_reason and (part, row.record_id) in NO_SENTINEL_PINS:
            if line in ADDITIONAL_INFO:
                raise ParseError(f"{ctx}: sub-heading before narrative")
            in_reason = True
            reason_parts.append(line)
            continue
        raise ParseError(f"{ctx}: unrecognized field line {line[:60]!r}")
    if not in_reason:
        raise ParseError(f"{ctx}: entry has no narrative reason")
    if not reason_parts:
        raise ParseError(f"{ctx}: entry has no narrative reason")
    if not row.start_date:
        raise ParseError(f"{ctx}: entry has no UN designation date")
    row.reason = " ".join(reason_parts)


def parse_annex_i(roman: str, block: Element) -> list[Row]:
    part_index = -1
    seen_subtitle = False
    at_entry = False
    entries: list[tuple[int, str, list[str]]] = []  # (part index, heading, lines)
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
        text = clean(element_text(child), roman)
        if child.tag == "p" and cls == "title-gr-seq-level-1":
            if seen_subtitle or text != I_SUBTITLE:
                raise ParseError(f"{roman}: unexpected annex subtitle {text[:60]!r}")
            seen_subtitle = True
            continue
        if child.tag == "p" and cls == "title-gr-seq-level-2":
            if part_index + 1 >= len(I_PARTS) or text != I_PARTS[part_index + 1]:
                raise ParseError(f"{roman}: unexpected part heading {text!r}")
            part_index += 1
            at_entry = False
            continue
        if child.tag == "p" and cls == "title-gr-seq-level-3":
            if part_index != 0:
                # Section B prints no entries; no entry format is taught.
                raise ParseError(f"{roman}: entry outside A. PERSONS: {text[:50]!r}")
            entries.append((part_index, text, []))
            at_entry = True
            continue
        if child.tag == "p" and cls == "norm":
            if not at_entry:
                raise ParseError(f"{roman}: text before first entry: {text[:50]!r}")
            entries[-1][2].append(text)
            continue
        raise ParseError(f"{roman}: unexpected <{child.tag} class={cls!r}>")
    if not seen_subtitle:
        raise ParseError(f"{roman}: annex subtitle missing")
    if part_index + 1 != len(I_PARTS):
        raise ParseError(f"{roman}: saw {part_index + 1} parts, expected 2")
    rows: list[Row] = []
    for _, heading, lines in entries:
        ctx = f"{roman}.A"
        row = parse_heading(ctx, "A", heading)
        parse_fields(f"{ctx} entry {row.record_id}", "A", row, lines)
        rows.append(row)
    return rows


def parse_ii_row(ctx: str, cells: list[Element]) -> Row:
    record_id = cell_line(cells[0], ctx)
    match = NUMBER_RE.match(record_id)
    if match is None:
        raise ParseError(f"{ctx}: unrecognized entry number {record_id!r}")
    number = match.group(1)
    ctx = f"{ctx} entry {number}"
    schema = II_ENTRY_SCHEMAS.get(number)
    if schema is None:
        raise ParseError(f"{ctx}: no reviewed schema for entry {number}")
    row = Row("II", schema, MEASURE, record_id=number)
    row.add("name", [cell_line(cells[1], ctx)])
    for line in cell_lines(cells[2], ctx):
        labelled = LABELLED_RE.match(line)
        if labelled is None or labelled.group(1) not in II_INFO_LABELS:
            raise ParseError(f"{ctx}: unrecognized info line {line[:60]!r}")
        column = II_INFO_LABELS[labelled.group(1)]
        row.add(column, split_values(labelled.group(2)))
    reason_lines = cell_lines(cells[3], ctx)
    if not reason_lines:
        raise ParseError(f"{ctx}: empty reasons cell")
    row.reason = " ".join(reason_lines)
    row.start_date = verbatim_date(cell_line(cells[4], ctx), ctx)
    return row


def parse_annex_ii(roman: str, block: Element) -> list[Row]:
    seen_subtitle = False
    tables: list[Element] = []
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
            text = clean(element_text(child), roman)
            if seen_subtitle or text != II_SUBTITLE:
                raise ParseError(f"{roman}: unexpected annex subtitle {text[:60]!r}")
            seen_subtitle = True
            continue
        if child.tag == "div" and cls == "centered":
            tables.extend(xpath_elements(child, ".//table"))
            continue
        raise ParseError(f"{roman}: unexpected <{child.tag} class={cls!r}>")
    if not seen_subtitle:
        raise ParseError(f"{roman}: annex subtitle missing")
    if len(tables) != 1:
        raise ParseError(f"{roman}: expected one table, found {len(tables)}")
    rows: list[Row] = []
    for tr in table_body(roman, tables[0], II_HEADER):
        cells = xpath_elements(tr, "./td|./th")
        rows.append(parse_ii_row(roman, cells))
    return rows


def check_registry() -> None:
    program = get_program_by_key(PROGRAM_KEY)
    if program is None:
        raise ParseError(f"unknown program key {PROGRAM_KEY!r}")
    if MEASURE not in get_args(Measure):
        raise ParseError(f"invalid measure {MEASURE!r}")
    if MEASURE not in program.measures:
        raise ParseError(f"measure {MEASURE!r} not in {PROGRAM_KEY}")
    for schema_name in {I_SCHEMA, *II_ENTRY_SCHEMAS.values()}:
        if model.get(schema_name) is None:
            raise ParseError(f"unknown schema {schema_name!r}")


def parse_document(doc: Element) -> list[Row]:
    rows: list[Row] = []
    for roman, block in annex_blocks(doc, {"I", "II"} | NON_TARGET):
        if roman in NON_TARGET:
            continue
        if roman == "I":
            annex_rows = parse_annex_i(roman, block)
        else:
            annex_rows = parse_annex_ii(roman, block)
        if not annex_rows:
            raise ParseError(f"{roman}: no entries extracted")
        rows.extend(annex_rows)
    return rows


@click.command(help="Parse consolidated Regulation 2015/735 into a CSV candidate.")
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
            raise ParseError(f"not a consolidated 2015/735 CELEX: {celex!r}")
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
