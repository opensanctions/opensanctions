"""Parse consolidated Regulation (EU) 747/2014 (Sudan) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation's target annex:

- Annex I — persons designated under the UN Darfur sanctions regime
  (UNSCR 1591, Article 5), in the UN line-oriented layout: part
  A. Natural persons holds numbered name headings followed by one labelled
  field line per paragraph, an optional "Reason for listing:" block, and
  the narrative "Information from the narrative summary of reasons for
  listing provided by the Sanctions Committee:" section — together the
  reason for listing. Part B. Legal persons, entities and bodies prints
  its heading with no entries; the UN committee has never designated an
  entity, and an entry appearing there breaks the run for review. Travel
  bans live in Decision 2014/450/CFSP.

Annex II lists competent-authority websites, not designations. Delisted
entries leave numbering gaps. The trailing sentence period the newer
entries print after each field value is run-on punctuation, not part of
the value, and is stripped from structured values and the designation
date only; reason and notes prose keeps printed punctuation. The
"Related listed individuals and entities" sub-heading names other
designees — relational content with no CSV column, deliberately not
transcribed. Dates are transcribed as the source prints them
("25 April 2006", "1 Jan. 1964"); the crawler normalizes dates.

Output: data/consolidated/32014R0747.csv (the EU Journal consolidated CSV
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
    SKIP_P_CLASSES,
    ParseError,
    Row,
    annex_blocks,
    annex_id,
    check_marker,
    clean,
    load_source,
    parse_worded_date,
    summary,
    to_record,
    validate_records,
    write_csv,
)
from followthemoney import model
from lxml import html
from zavod.helpers.html import element_text
from zavod.stateful.programs import Measure, get_program_by_key
from zavod.util import Element

FRAMEWORK_CELEX = "32014R0747"
CONSOLIDATED_RE = re.compile(r"^02014R0747-\d{8}$")
PROGRAM_KEY = "EU-SDN"
# Annex I implements the Article 5 fund freeze; travel bans live in
# Decision 2014/450/CFSP.
MEASURE = "Asset freeze"

NON_TARGET = frozenset({"II"})

# (printed part heading, part id, schema) in print order.
PARTS = (
    ("A. Natural persons", "A", "Person"),
    ("B. Legal persons, entities and bodies", "B", "LegalEntity"),
)
# Parts that print their heading with no entries; an entry appearing there
# is a new, untaught format and breaks for review.
EXPECTED_EMPTY_PARTS = frozenset({"B"})

# Entry headings: "1. ELHASSAN, Gaffar Mohammed".
HEADING_RE = re.compile(r"^(\d+)\. (.+)$")

# Labelled field lines → CSV column, with the observed casing variants.
FIELD_COLUMNS = {
    "Alias": "alias",
    "Designation": "position",
    "Date of birth": "birthDate",
    "Place of birth": "birthPlace",
    "Nationality": "nationality",
    "Gender": "gender",
    "Passport": "passportNumber",
    "National identification no": "idNumber",
    "National identification No": "idNumber",
    "National Identification Number": "idNumber",
    "Address": "address",
}
DATE_LABEL = "Date of UN designation"
# "Other information:" holds descriptive prose → one bare notes value.
NOTES_LABEL = "Other information"
LABELLED_LINE_RE = re.compile(r"^([A-Za-z][A-Za-z .]{2,40}?):\s*(.*)$")

# Everything after either sentinel is the source's reason for listing: the
# newer entries print a "Reason for listing:" sentence block before the
# narrative summary, the original entries only the narrative summary.
REASON_SENTINELS = frozenset(
    {
        "Reason for listing:",
        "Information from the narrative summary of reasons for listing "
        "provided by the Sanctions Committee:",
    }
)
# Sub-heading naming other designees; relational content with no CSV
# column. The heading and every line after it are deliberately dropped.
RELATED_HEADING = "Related listed individuals and entities"

# Reviewed hand-mappings for field lines the label rules cannot place,
# keyed by (part, entry) and the exact line. If the source line changes,
# the lookup misses and the run breaks for re-review.
INFO_OVERRIDES: dict[tuple[str, str], dict[str, tuple[tuple[str, str], ...]]] = {
    # The alias label is printed without its colon.
    ("A", "7"): {"Alias Abu Nashuk": (("alias", "Abu Nashuk"),)},
    # The passport list wraps onto a bare continuation line.
    ("A", "2"): {
        (
            "Passport: a) Diplomatic Passport D014433, issued on "
            "21 Feb. 2013 (Expired on 21 Feb. 2015);"
        ): (
            (
                "passportNumber",
                "Diplomatic Passport D014433, issued on 21 Feb. 2013 "
                "(Expired on 21 Feb. 2015)",
            ),
        ),
        (
            "b) Diplomatic Passport D009889, issued on 17 Feb. 2011 "
            "(Expired on 17 Feb. 2013)"
        ): (
            (
                "passportNumber",
                "Diplomatic Passport D009889, issued on 17 Feb. 2011 "
                "(Expired on 17 Feb. 2013)",
            ),
        ),
    },
}


def verbatim_date(text: str, ctx: str) -> str:
    # Only worded dates ("25 April 2006") occur in this document. The
    # printed wording is kept; the recognizer only guards the shape.
    if parse_worded_date(text) is None:
        raise ParseError(f"{ctx}: unrecognized date {text!r}")
    return text


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
        items.append(value[start + 3 : end].strip())
    return items


def split_paren_lettered(ctx: str, value: str) -> list[str]:
    """Split a UN "(a) …; (b) …" list, checking the letters run in sequence."""
    pieces = re.split(r"(?:^|; )\(([a-z])\) ", value)
    if pieces[0] != "":
        raise ParseError(f"{ctx}: unparsable lettered list {value[:60]!r}")
    letters = pieces[1::2]
    items = [item.strip() for item in pieces[2::2]]
    expected = [chr(ord("a") + i) for i in range(len(letters))]
    if letters != expected or len(items) != len(letters):
        raise ParseError(f"{ctx}: lettered markers {letters} out of sequence")
    return items


def field_values(ctx: str, value: str) -> list[str]:
    """Expand one structured field into CSV values.

    The newer entries close every field with a sentence period, and
    mid-list items keep their "; " or ", " separator — run-on punctuation,
    not value content; one trailing "." or ";" is stripped from the value
    and one trailing ";" or "," from each split item.
    """
    if value.endswith(".") or value.endswith(";"):
        value = value[:-1].strip()
    if value.startswith("(a) "):
        items = split_paren_lettered(ctx, value)
    elif value.startswith("a) "):
        items = split_plain_lettered(ctx, value)
    else:
        items = [value]
    cleaned: list[str] = []
    for item in items:
        item = item.rstrip(";,").strip()
        if not item:
            raise ParseError(f"{ctx}: empty item in {value[:60]!r}")
        cleaned.append(item)
    return cleaned


def alias_values(ctx: str, value: str) -> list[str]:
    """Alias items shed a full ‘…’ quote wrap (printed emphasis, not name)."""
    items: list[str] = []
    for item in field_values(ctx, value):
        if item.startswith("‘") and item.endswith("’"):
            item = item[1:-1].strip()
            if not item:
                raise ParseError(f"{ctx}: empty quoted alias")
        items.append(item)
    return items


def parse_heading(ctx: str, part: str, schema: str, text: str) -> Row:
    match = HEADING_RE.match(text)
    if match is None:
        raise ParseError(f"{ctx}: unrecognized entry heading {text[:60]!r}")
    record_id = match.group(1)
    row = Row(annex_id("I", part), schema, MEASURE, record_id=record_id)
    row.add("name", [match.group(2)])
    return row


def parse_fields(ctx: str, part: str, row: Row, lines: list[str]) -> None:
    overrides = INFO_OVERRIDES.get((part, row.record_id), {})
    in_notes = False
    in_reason = False
    dropping = False
    seen_sentinels: set[str] = set()
    reason_parts: list[str] = []
    for line in lines:
        if line == RELATED_HEADING:
            dropping = True
            continue
        if dropping:
            continue
        if line in REASON_SENTINELS:
            if line in seen_sentinels:
                raise ParseError(f"{ctx}: repeated sentinel {line[:40]!r}")
            seen_sentinels.add(line)
            in_reason = True
            continue
        if in_reason:
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
            row.start_date = verbatim_date(value.removesuffix("."), ctx)
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
            column = FIELD_COLUMNS[label]
            if column == "alias":
                row.add(column, alias_values(ctx, value))
            else:
                row.add(column, field_values(ctx, value))
            continue
        if in_notes:
            # Bare paragraphs continue the Other information block.
            row.add("notes", [line])
            continue
        raise ParseError(f"{ctx}: unrecognized field line {line[:60]!r}")
    if not in_reason:
        raise ParseError(f"{ctx}: entry has no reason sentinel")
    if not reason_parts:
        raise ParseError(f"{ctx}: entry has no reason text")
    if not row.start_date:
        raise ParseError(f"{ctx}: entry has no UN designation date")
    row.reason = " ".join(reason_parts)


def parse_annex_i(roman: str, block: Element) -> list[Row]:
    part_index = -1
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
            if part_index + 1 >= len(PARTS) or text != PARTS[part_index + 1][0]:
                raise ParseError(f"{roman}: unexpected part heading {text!r}")
            part_index += 1
            at_entry = False
            continue
        if child.tag == "p" and cls == "title-gr-seq-level-2":
            if part_index < 0:
                raise ParseError(f"{roman}: entry before first part heading")
            part = PARTS[part_index][1]
            if part in EXPECTED_EMPTY_PARTS:
                raise ParseError(f"{roman}.{part}: entry in part taught as empty")
            entries.append((part_index, text, []))
            at_entry = True
            continue
        if child.tag == "p" and cls == "title-gr-seq-level-3":
            if not at_entry or text not in REASON_SENTINELS:
                raise ParseError(f"{roman}: unexpected sub-heading {text[:50]!r}")
            entries[-1][2].append(text)
            continue
        if child.tag == "p" and cls == "title-gr-seq-level-4":
            if not at_entry or text != RELATED_HEADING:
                raise ParseError(f"{roman}: unexpected sub-heading {text[:50]!r}")
            entries[-1][2].append(text)
            continue
        if child.tag == "p" and cls == "norm":
            if not at_entry:
                raise ParseError(f"{roman}: text before first entry: {text[:50]!r}")
            entries[-1][2].append(text)
            continue
        raise ParseError(f"{roman}: unexpected <{child.tag} class={cls!r}>")
    if part_index + 1 != len(PARTS):
        raise ParseError(f"{roman}: saw {part_index + 1} parts, expected {len(PARTS)}")
    rows: list[Row] = []
    last_number = 0
    for index, heading, lines in entries:
        _, part, schema = PARTS[index]
        ctx = f"{roman}.{part}"
        row = parse_heading(ctx, part, schema, heading)
        number = int(row.record_id)
        if number <= last_number:
            raise ParseError(f"{ctx}: entry number {number} out of order")
        last_number = number
        parse_fields(f"{ctx} entry {row.record_id}", part, row, lines)
        rows.append(row)
    return rows


def check_registry() -> None:
    program = get_program_by_key(PROGRAM_KEY)
    if program is None:
        raise ParseError(f"unknown program key {PROGRAM_KEY!r}")
    if MEASURE not in get_args(Measure):
        raise ParseError(f"invalid measure {MEASURE!r}")
    if MEASURE not in program.measures:
        raise ParseError(f"measure {MEASURE!r} not in {PROGRAM_KEY}")
    for _, _, schema_name in PARTS:
        if model.get(schema_name) is None:
            raise ParseError(f"unknown schema {schema_name!r}")


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


@click.command(help="Parse consolidated Regulation 747/2014 into a CSV candidate.")
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
            raise ParseError(f"not a consolidated 747/2014 CELEX: {celex!r}")
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
