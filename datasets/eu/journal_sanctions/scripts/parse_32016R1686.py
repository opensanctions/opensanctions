"""Parse consolidated Regulation (EU) 2016/1686 (ISIL/Al-Qaida, EU track)
into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation's annexes:

- Annex I — the Article 3 fund-freeze list of the EU-autonomous ISIL
  (Da'esh)/Al-Qaida regime (the UN track lives in Regulation (EC) No
  881/2002). Not a table: each entry is one prose sentence in its own div —
  part A (natural persons) as a numbered paragraph, part B (legal persons,
  entities and bodies) as a grid-list container. The sentence holds the
  name, an optional "(a.k.a. …)" parenthetical, and semicolon-separated
  labelled fields ("date of birth: 1977"). No per-designation listing date
  or reason is printed, so startDate and reason stay empty — never infer
  them. Travel bans ride on Decision (CFSP) 2016/1693.
- Annex II — competent-authority websites, not designations.

Delisted entries leave numbering gaps. The "(a.k.a. …)" content is kept
whole as one alias value: its items are comma-joined and the contract splits
alias lists on ";" only; the crawler's name review categorises the pieces.
Field values are transcribed as printed ("1978 or 1984", "Pakistani
(presumed)", "French, Turkish"); the crawler normalizes them.

Output: data/consolidated/32016R1686.csv (the EU Journal consolidated CSV
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
    ParseError,
    Row,
    annex_blocks,
    check_marker,
    clean,
    load_source,
    summary,
    to_record,
    validate_records,
    write_csv,
)
from followthemoney import model
from lxml import html
from zavod.helpers.html import element_text, xpath_elements
from zavod.stateful.programs import Measure, get_program_by_key
from zavod.util import Element

FRAMEWORK_CELEX = "32016R1686"
CONSOLIDATED_RE = re.compile(r"^02016R1686-\d{8}$")
PROGRAM_KEY = "EU-TAQA-EUAQ"
# Annex I implements the Article 3 fund freeze; travel bans live in
# Decision (CFSP) 2016/1693.
MEASURE = "Asset freeze"

TARGET_ANNEX = "I"
NON_TARGET = frozenset({"II"})

SUBTITLE = (
    "List of natural and legal persons, entities and bodies referred to in Article 3"
)
# (part heading, part id, schema) in print order.
PARTS = (
    ("A. Natural persons referred to in Article 3", "A", "Person"),
    (
        "B. Legal persons, entities and bodies referred to in Article 3",
        "B",
        "LegalEntity",
    ),
)

ENTRY_RE = re.compile(r"^(\d+)\. (.+)$")
# The name's trailing alias parenthetical; the printed "a.k.a." label is
# what categorises the content as an alias.
NAME_AKA_RE = re.compile(r"^(.+) \(a\.k\.a\. (.+)\)$")

# Entry A18 prints a doubled alias label: "(a.k.a. also known as …)". The
# redundant second label is stripped with the first; if the source changes,
# the prefix check misses and the run breaks for re-review.
AKA_PREFIX_PINS = {("A", "18"): "also known as "}

# Printed field labels; the document capitalizes "Nationality" once (A2).
INFO_LABELS = {
    "date of birth": "birthDate",
    "place of birth": "birthPlace",
    "nationality": "nationality",
    "Nationality": "nationality",
    "gender": "gender",
    "passport number": "passportNumber",
    "identity card number": "idNumber",
}


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


def parse_entry(part: str, prose: str, row: Row) -> None:
    ctx = f"I.{part} entry {row.record_id}"
    if not prose.endswith("."):
        raise ParseError(f"{ctx}: entry does not end with a period")
    segments = prose[:-1].split("; ")
    name = segments[0]
    aka = NAME_AKA_RE.match(name)
    if aka is not None:
        name, aliases = aka.group(1), aka.group(2)
        prefix = AKA_PREFIX_PINS.get((part, row.record_id))
        if prefix is not None:
            if not aliases.startswith(prefix):
                raise ParseError(f"{ctx}: pinned alias prefix missing")
            aliases = aliases[len(prefix) :]
        row.add("alias", [aliases])
    if "a.k.a" in name:
        raise ParseError(f"{ctx}: alias label left in name {name[:60]!r}")
    row.add("name", [name])
    for segment in segments[1:]:
        labelled = LABELLED_RE.match(segment)
        if labelled is None:
            raise ParseError(f"{ctx}: unrecognized field {segment[:60]!r}")
        label, value = labelled.group(1), labelled.group(2)
        if label not in INFO_LABELS or value == "":
            raise ParseError(f"{ctx}: unrecognized field {segment[:60]!r}")
        row.add(INFO_LABELS[label], [value])


def entry_prose(ctx: str, entry: Element, part: str) -> tuple[str, str]:
    """Return (number, prose), checking the entry's printed structure."""
    whole = clean(element_text(entry), ctx)
    match = ENTRY_RE.match(whole)
    if match is None:
        raise ParseError(f"{ctx}: unrecognized entry text {whole[:60]!r}")
    number, prose = match.group(1), match.group(2)
    if part == "A":
        # One p.norm holds the whole entry: its own text node is the number
        # and a child span's tail carries the prose.
        paragraphs = xpath_elements(entry, "./p")
        if len(paragraphs) != 1 or paragraphs[0].get("class") != "norm":
            raise ParseError(f"{ctx}: unexpected paragraphs in entry {number}")
        if (paragraphs[0].text or "").strip() != f"{number}.":
            raise ParseError(f"{ctx}: number text mismatch in entry {number}")
        if clean(element_text(paragraphs[0]), ctx) != whole:
            raise ParseError(f"{ctx}: prose paragraph mismatch in entry {number}")
    else:
        column = xpath_elements(
            entry, "./div[@class='grid-list-column-2']", expect_exactly=1
        )[0]
        paragraphs = xpath_elements(column, "./p")
        if len(paragraphs) != 1 or paragraphs[0].get("class") != "norm":
            raise ParseError(f"{ctx}: unexpected paragraphs in entry {number}")
        if clean(element_text(paragraphs[0]), ctx) != prose:
            raise ParseError(f"{ctx}: prose paragraph mismatch in entry {number}")
    return number, prose


def parse_annex_i(annex: str, block: Element) -> list[Row]:
    rows: list[Row] = []
    part = ""
    schema = ""
    part_index = 0
    last_number = 0
    for child in block.iterchildren():
        if not isinstance(child.tag, str):
            continue
        cls = child.get("class") or ""
        if child.tag == "hr" and cls == "separator-annex":
            continue
        if child.tag == "p" and cls == "modref":
            check_marker(" ".join(element_text(child).split()), annex)
            continue
        if child.tag == "p" and cls in ("", "title-annex-1"):
            if cls == "" and clean(element_text(child), annex) != "":
                raise ParseError(f"{annex}: unexpected paragraph content")
            continue
        if child.tag == "p" and cls == "title-gr-seq-level-1":
            if clean(element_text(child), annex) != SUBTITLE:
                raise ParseError(f"{annex}: unexpected annex subtitle")
            continue
        if child.tag == "p" and cls == "title-gr-seq-level-2":
            heading = clean(element_text(child), annex)
            if part_index >= len(PARTS) or heading != PARTS[part_index][0]:
                raise ParseError(f"{annex}: unexpected part heading {heading!r}")
            _, part, schema = PARTS[part_index]
            part_index += 1
            last_number = 0
            continue
        if child.tag == "div" and cls in ("", "grid-container grid-list"):
            expected = "" if part == "A" else "grid-container grid-list"
            if part == "" or cls != expected:
                raise ParseError(f"{annex}: entry container out of place")
            ctx = f"{annex}.{part}"
            number, prose = entry_prose(ctx, child, part)
            if int(number) <= last_number:
                raise ParseError(f"{ctx}: entry {number} out of order")
            last_number = int(number)
            row = Row(f"{annex}.{part}", schema, MEASURE, record_id=number)
            parse_entry(part, prose, row)
            rows.append(row)
            continue
        raise ParseError(f"{annex}: unexpected <{child.tag} class={cls!r}>")
    if part_index != len(PARTS):
        raise ParseError(f"{annex}: saw {part_index} parts, expected {len(PARTS)}")
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


@click.command(help="Parse consolidated Regulation 2016/1686 into a CSV candidate.")
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
            raise ParseError(f"not a consolidated 2016/1686 CELEX: {celex!r}")
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
