"""Parse consolidated Regulation (EU) 224/2014 (Central African Republic) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation's target annex:

- Annex I — persons and entities designated under the UN CAR sanctions
  regime (Article 5), in the UN paragraph layout: parts A. Persons and
  B. Entities, each entry a numbered name heading with an "(alias: …)"
  parenthetical, one labelled field line per paragraph, then the narrative
  "Information from the narrative summary of reasons for listing provided
  by the Sanctions Committee:" section, which is the reason for listing.
  Travel bans live in Decision 2013/798/CFSP.

Annex II lists competent-authority websites, not designations. Delisted
entries leave numbering gaps. Dates are transcribed as the source prints
them ("9 May 2014", "20 Aug. 2015"); the crawler normalizes dates.

Output: data/consolidated/32014R0224.csv (the EU Journal consolidated CSV
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
    annex_id,
    check_consolidated_celex,
    check_marker,
    check_registry,
    clean,
    load_source,
    summary,
    to_record,
    validate_records,
    verbatim_date,
    write_csv,
)
from lxml import html
from zavod.helpers.html import element_text
from zavod.util import Element

FRAMEWORK_CELEX = "32014R0224"
PROGRAM_KEY = "EU-CAF"
# Annex I implements the Article 5 fund freeze; travel bans live in
# Decision 2013/798/CFSP.
MEASURE = "Asset freeze"

NON_TARGET = frozenset({"II"})

# (printed part heading, part id, schema) in print order.
PARTS = (
    ("A. Persons", "A", "Person"),
    ("B. Entities", "B", "LegalEntity"),
)

# Entry headings: "1. Francois Yangouvonda BOZIZÉ (alias : a) …)". Every
# observed entry carries the alias parenthetical; both label spacings occur.
HEADING_RE = re.compile(r"^(\d+)\. (.+?) \(alias ?: (.+)$")

# Labelled field lines → CSV column, with the observed casing variants.
FIELD_COLUMNS = {
    "Title": "position",
    "Designation": "position",
    "Date of birth": "birthDate",
    "Date of Birth": "birthDate",
    "Place of birth": "birthPlace",
    "Place of Birth": "birthPlace",
    "Nationality": "nationality",
    "Passport no": "passportNumber",
    "Passport No": "passportNumber",
    "National identification no": "idNumber",
    "National identification No": "idNumber",
    "National Identification No.": "idNumber",
    "Address": "address",
}
DATE_LABEL = "Date of UN designation"
# "Other information:" (one entry prints "Other Information:") opens a block
# of notes paragraphs that runs until the narrative sentinel; the value may
# sit on the label line, on following bare lines, or both.
NOTES_LABELS = frozenset({"Other information", "Other Information"})
LABELLED_LINE_RE = re.compile(r"^([A-Za-z][A-Za-z .]{2,40}?):\s*(.*)$")

# Everything after this line is the source's reason for listing.
NARRATIVE_SENTINEL = (
    "Information from the narrative summary of reasons for listing "
    "provided by the Sanctions Committee:"
)
# Sub-headings inside the narrative, printed both as norm paragraphs and as
# title-gr-seq-level-3 headings; they carry no content.
ADDITIONAL_INFO = frozenset({"Additional information", "Additional information:"})

# Entries whose "Date of UN designation" prints a trailing period
# ("7 March 2016."); the period is sentence punctuation, not part of the
# date, and is stripped for these pinned entries only.
DATE_PERIOD_PINS = frozenset({("A", "8"), ("A", "9"), ("A", "10"), ("B", "2")})

# Reviewed hand-mappings for field lines that mix two facts in one printed
# line, keyed by (part, entry) and the exact line. If the source line
# changes, the lookup misses and the run breaks for re-review.
INFO_OVERRIDES: dict[tuple[str, str], dict[str, tuple[tuple[str, str], ...]]] = {
    ("A", "6"): {
        (
            "Nationality: Sudan, CAR diplomatic passport No D00000898, "
            "issued on 11 April 2013 (valid until 10 April 2018)."
        ): (
            ("nationality", "Sudan"),
            (
                "passportNumber",
                "CAR diplomatic passport No D00000898, issued on "
                "11 April 2013 (valid until 10 April 2018)",
            ),
        ),
    },
}


# Formats observed in this document: worded dates ("9 May 2014") and UN
# abbreviated dates ("20 Aug. 2015").
DATE_FORMATS = (
    "worded",
    "abbrev",
)


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
    """Expand one field into CSV values; na/n/a are printed placeholders."""
    if value.startswith("(a) "):
        items = split_paren_lettered(ctx, value)
    elif value.startswith("a) "):
        items = split_plain_lettered(ctx, value)
    else:
        items = [value]
    return [item for item in items if item not in ("na", "n/a")]


def split_aliases(ctx: str, blob: str) -> list[str]:
    """Split the heading's alias parenthetical into alias values.

    The parenthetical's closing paren is inconsistently printed (missing,
    doubled mid-list, or followed by a period); an item left with more
    closing than opening parens sheds one trailing paren.
    """
    if blob.endswith(")."):
        blob = blob[:-1]
    items = []
    for item in field_values(ctx, blob):
        item = item.rstrip(";").strip()
        if item.endswith(")") and item.count(")") > item.count("("):
            item = item[:-1].strip()
        if not item:
            raise ParseError(f"{ctx}: empty alias item in {blob[:60]!r}")
        items.append(item)
    return items


def parse_heading(ctx: str, part: str, schema: str, text: str) -> Row:
    match = HEADING_RE.match(text)
    if match is None:
        raise ParseError(f"{ctx}: unrecognized entry heading {text[:60]!r}")
    record_id = match.group(1)
    entry_ctx = f"{ctx} entry {record_id}"
    row = Row(annex_id("I", part), schema, MEASURE, record_id=record_id)
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
            if (part, row.record_id) in DATE_PERIOD_PINS:
                value = value.removesuffix(".")
            row.start_date = verbatim_date(value, ctx, DATE_FORMATS)
            continue
        if label in NOTES_LABELS:
            value = labelled.group(2) if labelled is not None else ""
            if value:
                row.add("notes", [value])
            in_notes = True
            continue
        if label in FIELD_COLUMNS:
            value = labelled.group(2) if labelled is not None else ""
            if not value:
                raise ParseError(f"{ctx}: empty value for label {label!r}")
            row.add(FIELD_COLUMNS[label], field_values(ctx, value))
            continue
        if in_notes:
            # Bare paragraphs continue the Other information block.
            row.add("notes", [line])
            continue
        raise ParseError(f"{ctx}: unrecognized field line {line[:60]!r}")
    if not in_reason:
        raise ParseError(f"{ctx}: entry has no narrative sentinel")
    if not reason_parts:
        raise ParseError(f"{ctx}: entry has no narrative reason")
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
            entries.append((part_index, text, []))
            at_entry = True
            continue
        if child.tag == "p" and cls in ("norm", "title-gr-seq-level-3"):
            if not at_entry:
                raise ParseError(f"{roman}: text before first entry: {text[:50]!r}")
            entries[-1][2].append(text)
            continue
        raise ParseError(f"{roman}: unexpected <{child.tag} class={cls!r}>")
    if part_index + 1 != len(PARTS):
        raise ParseError(f"{roman}: saw {part_index + 1} parts, expected {len(PARTS)}")
    rows: list[Row] = []
    for index, heading, lines in entries:
        _, part, schema = PARTS[index]
        ctx = f"{roman}.{part}"
        row = parse_heading(ctx, part, schema, heading)
        parse_fields(f"{ctx} entry {row.record_id}", part, row, lines)
        rows.append(row)
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


@click.command(help="Parse consolidated Regulation 224/2014 into a CSV candidate.")
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
