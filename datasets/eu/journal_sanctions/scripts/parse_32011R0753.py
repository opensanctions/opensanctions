"""Parse consolidated Regulation (EU) 753/2011 (Taliban) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation implements the UN 1988 Committee's Taliban list. Annex I is
the Article 4 fund-freeze list, printed in a UN narrative layout: parts
"A. Individuals associated with the Taliban" and "B. Entities and other
groups and undertakings associated with the Taliban", each entry a numbered
name heading — "(1) Name (alias (a) …, (b) …)." — followed by one or two
run-on field paragraphs whose labels ("Title: … Grounds for listing: …
Date of UN designation: …") are separated by sentence periods, and an
optional block opened by the Sanctions Committee narrative-summary heading.
Annex II lists competent-authority websites, not designations. Travel bans
live in Decision 2011/486/CFSP, not in this regulation.

Field values are sliced at depth-zero label occurrences. The sentence
period (or semicolon) that closes a structured field is scaffolding of the
run-on paragraph and is stripped; reason and notes values keep the printed
punctuation. UN "(a) …, (b) …" enumerations split structured multi-value
fields; "na" marks an absent value. Grounds for listing and the
narrative-summary paragraphs form the reason; "Other information" and the
person-description labels go to notes verbatim with their label. Dates are
transcribed as the source prints them ("25.1.2001"); the crawler
normalizes dates.

Output: data/consolidated/32011R0753.csv (the EU Journal consolidated CSV
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
    parse_dotted_date,
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

FRAMEWORK_CELEX = "32011R0753"
CONSOLIDATED_RE = re.compile(r"^02011R0753-\d{8}$")
PROGRAM_KEY = "EU-AFG"
# Annex I implements the Article 4 fund freeze; travel bans live in
# Decision 2011/486/CFSP.
MEASURE = "Asset freeze"

NON_TARGET = frozenset({"II"})

SUBTITLE = (
    "LIST OF NATURAL AND LEGAL PERSONS, GROUPS, UNDERTAKINGS AND ENTITIES "
    "REFERRED TO IN ARTICLE 4"
)
# (part heading, part id, schema) in print order.
PARTS = (
    ("A. Individuals associated with the Taliban", "A", "Person"),
    (
        "B. Entities and other groups and undertakings associated with the Taliban",
        "B",
        "LegalEntity",
    ),
)

NARRATIVE_HEADING = (
    "Additional information from the narrative summary of reasons for "
    "listing provided by the Sanctions Committee:"
)

# Entry headings: "(1) Abdul Baqi Basir Awal Shah (alias Abdul Baqi)."
HEADING_RE = re.compile(r"^\((\d+)\) (.+)\.$")
ALIAS_TAIL_RE = re.compile(r"^(.+?) \(alias:? (.+)\)$")
# Entry B5's heading carries its listing date after the alias parenthetical;
# every other entry prints the date in the field paragraph.
HEADING_DATE_PINS = frozenset({("B", "5")})
HEADING_DATE_SEP = ". Date of UN designation: "

# Field labels of the run-on paragraphs, by treatment. Structured fields
# map to a column and get enumeration splitting and punctuation stripping;
# notes labels contribute their bare prose value; reason and date labels
# feed the scalar Sanction columns; drop labels are deliberately not
# transcribed (kin names are relational, marital status has no column).
FIELD_COLUMNS = {
    "Title": "position",
    "Additional title": "position",
    "Designation": "position",
    "Good quality a.k.a.": "alias",
    "Low quality a.k.a.": "weakAlias",
    "Date of birth": "birthDate",
    "Date of Birth": "birthDate",
    "DOB": "birthDate",
    "Place of birth": "birthPlace",
    "Place of Birth": "birthPlace",
    "POB": "birthPlace",
    "Nationality": "nationality",
    "Address": "address",
    "Passport No": "passportNumber",
    "Passport number": "passportNumber",
    "National identification No": "idNumber",
    "Pakistan National Tax Number": "taxNumber",
    "Afghan Money Service Provider License Number": "registrationNumber",
    "Physical description": "appearance",
    "Distinguishing physical marks": "appearance",
    "Ethnic background": "ethnicity",
    "Father's name": "fatherName",
}
NOTES_LABELS = frozenset({"Other information"})
DROP_LABELS = frozenset({"Brother's name", "Marital Status"})
REASON_LABELS = frozenset(
    {"Grounds for listing", "Grounds for Listing", "Ground for listings"}
)
DATE_LABELS = frozenset({"Date of UN designation", "Date of UN Designation"})
ALL_LABELS = (
    frozenset(FIELD_COLUMNS) | NOTES_LABELS | DROP_LABELS | REASON_LABELS | DATE_LABELS
)
# Labels that may occur more than once in one entry (observed: A112 prints
# two Other information runs, B1 three tax numbers).
REPEATABLE_LABELS = frozenset({"Other information", "Pakistan National Tax Number"})
NOT_AVAILABLE = frozenset({"na", "n/a"})

# Reviewed hand-mappings for sliced field values that mix columns, keyed by
# (part, entry, label) with the exact raw value they were reviewed against.
# If the printed value changes, the run breaks for re-review. An empty
# mapping drops the value deliberately.
FIELD_OVERRIDES: dict[tuple[str, str, str], tuple[str, tuple[tuple[str, str], ...]]] = {
    # The additional-title value carries the entry's INTERPOL notice
    # sentence, printed elsewhere inside Other information.
    (
        "A",
        "38",
        "Additional title",
    ): (
        "Hafiz. INTERPOL-UN Security Council Special Notice web link: "
        "https://www.interpol.int/en/notice/search/un/4665093",
        (
            ("position", "Hafiz"),
            (
                "notes",
                "INTERPOL-UN Security Council Special Notice web link: "
                "https://www.interpol.int/en/notice/search/un/4665093",
            ),
        ),
    ),
    # The licence value runs on into other-information prose.
    (
        "B",
        "1",
        "Afghan Money Service Provider License Number",
    ): (
        "044. Haji Khairullah Haji Sattar Money Exchange was used by Taliban "
        "leadership to transfer money to Taliban commanders to fund fighters "
        "and operations in Afghanistan as of 2011.",
        (
            ("registrationNumber", "044"),
            (
                "notes",
                "Haji Khairullah Haji Sattar Money Exchange was used by "
                "Taliban leadership to transfer money to Taliban commanders "
                "to fund fighters and operations in Afghanistan as of 2011.",
            ),
        ),
    ),
}
# Reviewed hand-mappings for whole unlabelled continuation lines, same
# contract as FIELD_OVERRIDES.
LINE_OVERRIDES: dict[tuple[str, str], dict[str, tuple[tuple[str, str], ...]]] = {
    # The continuation opens with a relational sentence naming other
    # designees; such lines have no CSV column and are not transcribed.
    ("B", "1"): {
        (
            "Associated with Abdul Sattar Abdul Manan and Khairullah "
            "Barakzai Khudai Nazar. INTERPOL-UN Security Council Special "
            "Notice web link: "
            "https://www.interpol.int/en/notice/search/une/5235593"
        ): (
            (
                "notes",
                "INTERPOL-UN Security Council Special Notice web link: "
                "https://www.interpol.int/en/notice/search/une/5235593",
            ),
        ),
    },
}


def verbatim_date(text: str, ctx: str) -> str:
    # The only format observed in this document is the dotted listing date
    # ("25.1.2001"). The printed wording is kept; the recognizer only
    # guards the shape.
    if parse_dotted_date(text) is None:
        raise ParseError(f"{ctx}: unrecognized date {text!r}")
    return text


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


def find_labels(line: str) -> list[tuple[int, int, str]]:
    """Depth-zero (position, spelling length, label) occurrences in order.

    Labels only count at parenthesis depth zero — alias parentheticals and
    branch-office lists embed their own colons.
    """
    found: list[tuple[int, int, str]] = []
    spellings = sorted(ALL_LABELS, key=len, reverse=True)
    depth = 0
    i = 0
    while i < len(line):
        char = line[i]
        if char == "(":
            depth += 1
        elif char == ")":
            depth = max(0, depth - 1)
        elif depth == 0 and (i == 0 or line[i - 1] == " "):
            for label in spellings:
                if line.startswith(label + ":", i):
                    found.append((i, len(label) + 1, label))
                    i += len(label)
                    break
        i += 1
    return found


def slice_fields(ctx: str, line: str) -> list[tuple[str, str]]:
    """Split a run-on field paragraph into (label, raw value) pairs."""
    found = find_labels(line)
    if not found:
        raise ParseError(f"{ctx}: no field labels in {line[:60]!r}")
    if found[0][0] != 0:
        raise ParseError(f"{ctx}: text before first label: {line[:60]!r}")
    fields: list[tuple[str, str]] = []
    for index, (position, length, label) in enumerate(found):
        end = found[index + 1][0] if index + 1 < len(found) else len(line)
        fields.append((label, line[position + length : end].strip()))
    return fields


def split_enumerated(value: str) -> list[str]:
    """Split a UN "(a) …, (b) …" enumeration at depth-zero letter markers."""
    if not value.startswith("(a) "):
        return [value]
    starts = [0]
    expected = "b"
    depth = 0
    i = 4
    while i < len(value) - 3:
        if (
            depth == 0
            and value[i - 1] == " "
            and value[i] == "("
            and value[i + 1] == expected
            and value[i + 2 : i + 4] == ") "
        ):
            starts.append(i)
            expected = chr(ord(expected) + 1)
            i += 4
            continue
        if value[i] == "(":
            depth += 1
        elif value[i] == ")":
            depth = max(0, depth - 1)
        i += 1
    items: list[str] = []
    for index, start in enumerate(starts):
        end = starts[index + 1] if index + 1 < len(starts) else len(value)
        items.append(value[start + 4 : end].strip().rstrip(",").strip())
    return items


def structured_values(raw: str) -> list[str]:
    """Expand a structured field into values; na means absent."""
    # The closing sentence period (or the semicolon separating B1's tax
    # numbers) is run-on-paragraph scaffolding, not part of the value.
    value = raw.strip()
    if value.endswith(".") or value.endswith(";"):
        value = value[:-1].strip()
    if value in NOT_AVAILABLE:
        return []
    return [item for item in split_enumerated(value) if item not in NOT_AVAILABLE]


def parse_heading(ctx: str, part: str, heading: str, row: Row) -> str:
    match = HEADING_RE.match(heading)
    if match is None:
        raise ParseError(f"{ctx}: unrecognized entry heading {heading[:60]!r}")
    record_id, body = match.groups()
    ctx = f"{ctx} entry {record_id}"
    if HEADING_DATE_SEP in body:
        if (part, record_id) not in HEADING_DATE_PINS:
            raise ParseError(f"{ctx}: unexpected date in heading")
        body, date = body.split(HEADING_DATE_SEP, 1)
        row.start_date = verbatim_date(date, ctx)
    alias_match = ALIAS_TAIL_RE.match(body)
    if alias_match is not None:
        body, alias_group = alias_match.groups()
        if " (alias" in body or " (alias" in alias_group:
            raise ParseError(f"{ctx}: nested alias groups in {heading[:60]!r}")
        row.add("alias", split_enumerated(alias_group))
    if "alias" in body:
        raise ParseError(f"{ctx}: unextracted alias in name {body[:60]!r}")
    row.add("name", [body])
    return record_id


def apply_field(
    ctx: str, part: str, record_id: str, label: str, raw: str, row: Row
) -> None:
    override = FIELD_OVERRIDES.get((part, record_id, label))
    if override is not None:
        expected, mapped = override
        if raw != expected:
            raise ParseError(f"{ctx}: override value changed for {label!r}")
        for column, value in mapped:
            row.add(column, [value])
        return
    if label in DATE_LABELS:
        if row.start_date:
            raise ParseError(f"{ctx}: second listing date")
        value = raw.strip().rstrip(".")
        row.start_date = verbatim_date(value, ctx)
        return
    if label in REASON_LABELS:
        if row.reason:
            raise ParseError(f"{ctx}: second grounds for listing")
        row.reason = raw.strip()
        return
    if label in DROP_LABELS:
        return
    if label in NOTES_LABELS:
        if raw.strip() == "":
            return
        row.add("notes", [raw.strip()])
        return
    values = structured_values(raw)
    if not values:
        if raw.strip() in NOT_AVAILABLE or raw.strip().rstrip(".") in NOT_AVAILABLE:
            return
        raise ParseError(f"{ctx}: empty value for label {label!r}")
    row.add(FIELD_COLUMNS[label], values)


def parse_entry(
    roman: str, part: str, schema: str, heading: str, lines: list[str]
) -> Row:
    row = Row(annex_id(roman, part), schema, MEASURE)
    ctx = f"{roman}.{part}"
    record_id = parse_heading(ctx, part, heading, row)
    row.record_id = record_id
    ctx = f"{ctx} entry {record_id}"
    if not lines:
        raise ParseError(f"{ctx}: entry has no field paragraph")
    narrative: list[str] = []
    in_narrative = False
    seen: set[str] = set()
    last_label = ""
    line_overrides = LINE_OVERRIDES.get((part, record_id), {})
    for line in lines:
        if line == NARRATIVE_HEADING:
            if in_narrative:
                raise ParseError(f"{ctx}: second narrative heading")
            in_narrative = True
            continue
        if in_narrative:
            if find_labels(line):
                raise ParseError(f"{ctx}: labelled narrative line {line[:60]!r}")
            narrative.append(line)
            continue
        if line in line_overrides:
            for column, value in line_overrides[line]:
                row.add(column, [value])
            continue
        if not find_labels(line):
            # An unlabelled paragraph continues the entry's Other
            # information run; each printed line is one notes value.
            if last_label != "Other information":
                raise ParseError(f"{ctx}: unlabelled line {line[:60]!r}")
            row.add("notes", [line])
            continue
        for label, raw in slice_fields(ctx, line):
            if label in seen and label not in REPEATABLE_LABELS:
                raise ParseError(f"{ctx}: repeated label {label!r}")
            seen.add(label)
            last_label = label
            apply_field(ctx, part, record_id, label, raw, row)
    if narrative:
        row.reason = " ".join([row.reason] + narrative if row.reason else narrative)
    if not row.start_date:
        raise ParseError(f"{ctx}: no listing date")
    return row


def parse_annex_i(roman: str, block: Element) -> list[Row]:
    rows: list[Row] = []
    part = ""
    schema = ""
    seen_parts: list[str] = []
    seen_subtitle = False
    heading: str | None = None
    lines: list[str] = []

    def flush() -> None:
        nonlocal heading
        if heading is not None:
            rows.append(parse_entry(roman, part, schema, heading, lines))
        heading = None
        lines.clear()

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
            flush()
            expected = [h for h, _, _ in PARTS]
            if len(seen_parts) >= len(PARTS) or text != expected[len(seen_parts)]:
                raise ParseError(f"{roman}: unexpected part heading {text!r}")
            _, part, schema = PARTS[len(seen_parts)]
            seen_parts.append(part)
            continue
        if child.tag == "p" and cls == "title-gr-seq-level-2":
            if not part:
                raise ParseError(f"{roman}: entry before first part heading")
            flush()
            heading = text
            continue
        if child.tag == "p" and cls == "norm":
            if not part:
                if text == SUBTITLE and not seen_subtitle:
                    seen_subtitle = True
                    continue
                raise ParseError(f"{roman}: text before parts: {text[:60]!r}")
            if heading is None:
                raise ParseError(f"{roman}: text before first entry: {text[:60]!r}")
            lines.append(text)
            continue
        raise ParseError(f"{roman}: unexpected <{child.tag} class={cls!r}>")
    flush()
    if not seen_subtitle:
        raise ParseError(f"{roman}: missing annex subtitle")
    if seen_parts != [p for _, p, _ in PARTS]:
        raise ParseError(f"{roman}: parts {seen_parts}")
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


@click.command(help="Parse consolidated Regulation 753/2011 into a CSV candidate.")
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
            raise ParseError(f"not a consolidated 753/2011 CELEX: {celex!r}")
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
