"""Parse consolidated Regulation (EU) 2022/2309 (Haiti) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation's target annexes:

- Annex I — persons and entities designated under the UN Haiti sanctions
  regime (Articles 2, 3 and 9), in a UN line layout: sections PERSONS and
  ENTITIES, each entry a numbered heading paragraph (name, optional
  "(a.k.a. …)" parenthetical, then the printed listing basis), labelled
  field lines, and narrative paragraphs after the "Additional information
  from the narrative summary …" sentinel. The heading basis and the
  narrative together are the reason. Two entries print all their fields
  run together inside the heading paragraph and are sliced at their
  labels; the entity entries print the sentinel inline with the first
  narrative paragraph. The ENTITIES section lists gangs and gang
  coalitions, emitted as Organization. The printed section names are the
  annex identifiers (I.PERSONS, I.ENTITIES); numbering restarts per
  section.
- Annex Ia — the EU-autonomous Article 4a list, five-column tables in
  parts A. Natural persons (Person) and B. Legal persons, entities and
  bodies (LegalEntity). Part B's identifying-information cell prints bare
  descriptive prose, which goes to notes.

Annex II lists competent-authority websites, not designations. Travel bans
live in Decision (CFSP) 2022/2319. Dates are transcribed as the source
prints them ("21 October 2022", "16.12.2024"); the crawler normalizes
dates.

Output: data/consolidated/32022R2309.csv (the EU Journal consolidated CSV
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

FRAMEWORK_CELEX = "32022R2309"
CONSOLIDATED_RE = re.compile(r"^02022R2309-\d{8}$")
PROGRAM_KEY = "EU-HTI"
# Annexes I and Ia implement the fund freeze; travel bans live in Decision
# (CFSP) 2022/2319.
MEASURE = "Asset freeze"

NON_TARGET = frozenset({"II"})

# --- Annex I (UN list) -------------------------------------------------------

# (printed section heading, schema) in print order. The ENTITIES section
# lists gangs and gang coalitions, not registered legal persons.
I_SECTIONS = (
    ("PERSONS", "Person"),
    ("ENTITIES", "Organization"),
)

# Entry headings: "2. Johnson ANDRE (a.k.a. Izo). Listed pursuant …" — the
# text after the name (and optional alias parenthetical) is the printed
# listing basis, which opens the reason. Entry 1 prints "a.k.a" without the
# final dot and runs straight into prose without a closing period.
I_ALIAS_HEADING_RE = re.compile(r"^(\d+)\. ([^(.]+?) \(a\.k\.a\.? ([^)]+)\)\.? (.+)$")
I_PLAIN_HEADING_RE = re.compile(r"^(\d+)\. ([^.]+?)\. (.+)$")

# Labelled field lines → CSV column, as printed.
I_FIELD_COLUMNS = {
    "Function": "position",
    "DOB": "birthDate",
    "POB": "birthPlace",
    "Nationality": "nationality",
    "National identification no": "idNumber",
    "Address": "address",
    "Gender": "gender",
    "A.k.a.": "alias",
}
I_DATE_LABEL = "Date of UN designation"
# Unstated fields print an en-dash placeholder ("DOB: –").
PLACEHOLDER = "–"

# Everything after this line is narrative reason; the entity entries print
# the first narrative paragraph inline on the sentinel line.
I_SENTINEL = (
    "Additional information from the narrative summary of reasons for "
    "listing provided by the Sanctions Committee:"
)

# Entries that print their whole field paragraph run together inside the
# heading paragraph (▼M7); their fields are sliced at the labels below.
I_RUNON_PINS = frozenset({("PERSONS", "6"), ("PERSONS", "7")})
# Entry 1's heading runs straight into its reason prose, with the name as
# the sentence subject ("Jimmy CHERIZIER (a.k.a ‘Barbeque’) has engaged
# …"); the reason keeps the whole printed sentence rather than a dangling
# fragment.
I_FULL_SENTENCE_PINS = frozenset({("PERSONS", "1")})
I_RUNON_LABELS = (
    "Function",
    "DOB",
    "POB",
    "Nationality",
    I_DATE_LABEL,
    "Gender",
)

# --- Annex Ia (EU-autonomous tables) -----------------------------------------

IA_SUBTITLE = (
    "List of natural and legal persons, entities and bodies referred to in Article 4a"
)
# (printed part heading, part id, schema) in print order.
IA_PARTS = (
    ("A. Natural persons", "A", "Person"),
    ("B. Legal persons, entities and bodies", "B", "LegalEntity"),
)
IA_HEADER = (
    "",
    "Name",
    "Identifying information",
    "Statement of reasons",
    "Date of listing",
)
IA_NUMBER_RE = re.compile(r"^(\d+)\.$")
# Name-cell lines after the name join into one "a.k.a. …" run; the list
# splits on ";" only (comma-joined pieces stay whole as one alias).
IA_AKA_RE = re.compile(r"^a\.k\.a\. (.+)$")
IA_FIELD_COLUMNS = {
    "Function": "position",
    "DOB": "birthDate",
    "Nationality": "nationality",
    "Gender": "gender",
    "Address": "address",
}
# Entry A6 wraps its Function value onto a bare parenthetical line; the
# line continues the position value.
IA_WRAP_PINS = frozenset({("A", "6")})


def verbatim_date(text: str, ctx: str) -> str:
    # Formats observed in this document: worded dates in Annex I
    # ("21 October 2022") and dotted dates in Annex Ia ("16.12.2024"). The
    # printed wording is kept; the recognizers only guard the shape.
    for parse in (parse_worded_date, parse_dotted_date):
        if parse(text) is not None:
            return text
    raise ParseError(f"{ctx}: unrecognized date {text!r}")


def unwrap_quotes(value: str) -> str:
    if value.startswith("‘") and value.endswith("’"):
        return value[1:-1]
    return value


def split_space_lettered(ctx: str, value: str) -> list[str]:
    """Split a UN "(a) … (b) …" list, checking the letters run in sequence."""
    pieces = re.split(r"(?:^| )\(([a-z])\) ", value)
    if pieces[0] != "":
        raise ParseError(f"{ctx}: unparsable lettered list {value[:60]!r}")
    letters = pieces[1::2]
    items = [item.strip() for item in pieces[2::2]]
    expected = [chr(ord("a") + i) for i in range(len(letters))]
    if letters != expected or len(items) != len(letters):
        raise ParseError(f"{ctx}: lettered markers {letters} out of sequence")
    return items


def parse_i_heading(ctx: str, section: str, schema: str, text: str) -> tuple[Row, str]:
    """Parse an entry heading into a Row and the printed listing basis."""
    alias_match = I_ALIAS_HEADING_RE.match(text)
    if alias_match is not None:
        record_id, name, alias, basis = alias_match.groups()
        row = Row(annex_id("I", section), schema, MEASURE, record_id=record_id)
        row.add("name", [name])
        row.add("alias", [unwrap_quotes(alias)])
        return row, basis
    plain_match = I_PLAIN_HEADING_RE.match(text)
    if plain_match is not None:
        record_id, name, basis = plain_match.groups()
        row = Row(annex_id("I", section), schema, MEASURE, record_id=record_id)
        row.add("name", [name])
        return row, basis
    raise ParseError(f"{ctx}: unrecognized entry heading {text[:60]!r}")


def apply_i_field(ctx: str, row: Row, label: str, value: str) -> None:
    if value == PLACEHOLDER:
        return
    if label == I_DATE_LABEL:
        row.start_date = verbatim_date(value, ctx)
        return
    if not value:
        raise ParseError(f"{ctx}: empty value for label {label!r}")
    if label == "A.k.a.":
        row.add("alias", split_space_lettered(ctx, value))
        return
    row.add(I_FIELD_COLUMNS[label], split_values(value))


def parse_i_runon(ctx: str, section: str, schema: str, text: str) -> Row:
    """Parse a ▼M7 entry whose fields run together in the heading paragraph."""
    matches = [
        match
        for match in re.finditer(
            r" (" + "|".join(re.escape(label) for label in I_RUNON_LABELS) + r"): ",
            text,
        )
    ]
    sentinel_at = text.find(" " + I_SENTINEL + " ")
    if not matches or sentinel_at < 0 or matches[-1].end() > sentinel_at:
        raise ParseError(f"{ctx}: unparsable run-on entry {text[:60]!r}")
    row, basis = parse_i_heading(ctx, section, schema, text[: matches[0].start()])
    ctx = f"{ctx} entry {row.record_id}"
    seen: set[str] = set()
    for index, match in enumerate(matches):
        label = match.group(1)
        if label in seen:
            raise ParseError(f"{ctx}: repeated label {label!r}")
        seen.add(label)
        end = matches[index + 1].start() if index + 1 < len(matches) else sentinel_at
        apply_i_field(ctx, row, label, text[match.end() : end].strip())
    narrative = text[sentinel_at + len(I_SENTINEL) + 2 :].strip()
    if not narrative:
        raise ParseError(f"{ctx}: run-on entry has no narrative")
    row.reason = f"{basis} {narrative}"
    return row


def finish_i_entry(ctx: str, row: Row, basis: str, narrative: list[str]) -> None:
    if not narrative:
        raise ParseError(f"{ctx}: entry has no narrative reason")
    if not row.start_date:
        raise ParseError(f"{ctx}: entry has no UN designation date")
    row.reason = " ".join([basis] + narrative)


def parse_annex_i(roman: str, block: Element) -> list[Row]:
    section_index = -1
    rows: list[Row] = []
    row: Row | None = None
    basis = ""
    narrative: list[str] = []
    in_reason = False

    def close_entry() -> None:
        nonlocal row
        if row is not None:
            finish_i_entry(f"{roman} entry {row.record_id}", row, basis, narrative)
            rows.append(row)
            row = None

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
            close_entry()
            if section_index + 1 >= len(I_SECTIONS):
                raise ParseError(f"{roman}: more section headings than sections")
            if text != I_SECTIONS[section_index + 1][0]:
                raise ParseError(f"{roman}: unexpected section heading {text!r}")
            section_index += 1
            continue
        if child.tag == "div":
            close_entry()
            if section_index < 0:
                raise ParseError(f"{roman}: entry before first section heading")
            section, schema = I_SECTIONS[section_index]
            norms = xpath_elements(child, "./p[@class='norm']", expect_exactly=1)
            heading = clean(element_text(norms[0]), roman)
            number = heading.split(".", 1)[0]
            if (section, number) in I_RUNON_PINS:
                rows.append(
                    parse_i_runon(f"{roman}.{section}", section, schema, heading)
                )
                continue
            row, basis = parse_i_heading(f"{roman}.{section}", section, schema, heading)
            if (section, number) in I_FULL_SENTENCE_PINS:
                basis = heading.split(". ", 1)[1]
            narrative, in_reason = [], False
            continue
        if row is None:
            raise ParseError(f"{roman}: text outside any entry: {text[:50]!r}")
        ctx = f"{roman} entry {row.record_id}"
        if child.tag == "p" and cls == "title-gr-seq-level-2":
            if text != I_SENTINEL or in_reason:
                raise ParseError(f"{ctx}: unexpected sub-heading {text[:50]!r}")
            in_reason = True
            continue
        if child.tag == "p" and cls in ("list", "norm"):
            if in_reason:
                narrative.append(text)
                continue
            if text == I_SENTINEL:
                in_reason = True
                continue
            if text.startswith(I_SENTINEL + " "):
                in_reason = True
                narrative.append(text[len(I_SENTINEL) + 1 :])
                continue
            labelled = LABELLED_RE.match(text)
            if labelled is None or (
                labelled.group(1) not in I_FIELD_COLUMNS
                and labelled.group(1) != I_DATE_LABEL
            ):
                raise ParseError(f"{ctx}: unrecognized field line {text[:60]!r}")
            apply_i_field(ctx, row, labelled.group(1), labelled.group(2))
            continue
        raise ParseError(f"{roman}: unexpected <{child.tag} class={cls!r}>")
    close_entry()
    if section_index + 1 != len(I_SECTIONS):
        raise ParseError(f"{roman}: saw {section_index + 1} sections")
    return rows


# --- Annex Ia ----------------------------------------------------------------


def parse_ia_name(ctx: str, td: Element, row: Row) -> None:
    lines = cell_lines(td, ctx)
    if not lines:
        raise ParseError(f"{ctx}: empty name cell")
    row.add("name", [lines[0]])
    if len(lines) == 1:
        return
    run = " ".join(lines[1:])
    aka = IA_AKA_RE.match(run)
    if aka is None:
        raise ParseError(f"{ctx}: unrecognized name-cell run {run[:60]!r}")
    aliases = [item.strip() for item in aka.group(1).split(";")]
    row.add("alias", [item for item in aliases if item])


def parse_ia_info(ctx: str, part: str, record_id: str, td: Element, row: Row) -> None:
    for line in cell_lines(td, ctx):
        labelled = LABELLED_RE.match(line)
        if labelled is not None and labelled.group(1) in IA_FIELD_COLUMNS:
            value = labelled.group(2)
            if not value:
                raise ParseError(f"{ctx}: empty value for {labelled.group(1)!r}")
            row.add(IA_FIELD_COLUMNS[labelled.group(1)], split_values(value))
            continue
        if part == "B":
            # Part B prints bare descriptive prose about the entity.
            row.add("notes", [line])
            continue
        if (part, record_id) in IA_WRAP_PINS and row.props.get("position"):
            # The Function value wraps onto a bare parenthetical line.
            row.props["position"][-1] = f"{row.props['position'][-1]} {line}"
            continue
        raise ParseError(f"{ctx}: unrecognized info line {line[:60]!r}")


def parse_ia_row(roman: str, part: str, schema: str, tr: Element) -> Row:
    cells = xpath_elements(tr, "./td|./th")
    ctx = f"{roman}.{part}"
    number = IA_NUMBER_RE.match(cell_line(cells[0], ctx))
    if number is None:
        raise ParseError(f"{ctx}: unrecognized entry number cell")
    record_id = number.group(1)
    ctx = f"{ctx} entry {record_id}"
    row = Row(annex_id(roman, part), schema, MEASURE, record_id=record_id)
    parse_ia_name(ctx, cells[1], row)
    parse_ia_info(ctx, part, record_id, cells[2], row)
    row.reason = " ".join(cell_lines(cells[3], ctx))
    row.start_date = verbatim_date(cell_line(cells[4], ctx), ctx)
    return row


def parse_annex_ia(roman: str, block: Element) -> list[Row]:
    rows: list[Row] = []
    part_index = -1
    part_tables = [0 for _ in IA_PARTS]
    seen_subtitle = False
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
        if child.tag == "p" and cls == "norm":
            if seen_subtitle or clean(element_text(child), roman) != IA_SUBTITLE:
                raise ParseError(f"{roman}: unexpected annex subtitle")
            seen_subtitle = True
            continue
        if child.tag == "p" and cls == "title-gr-seq-level-1":
            part_index += 1
            if part_index >= len(IA_PARTS):
                raise ParseError(f"{roman}: more part headings than parts")
            heading = clean(element_text(child), roman)
            if heading != IA_PARTS[part_index][0]:
                raise ParseError(f"{roman}: unexpected part heading {heading!r}")
            continue
        if child.tag == "div" and cls == "centered":
            if part_index < 0:
                raise ParseError(f"{roman}: table before first part heading")
            _, part, schema = IA_PARTS[part_index]
            part_tables[part_index] += 1
            table = xpath_elements(child, ".//table", expect_exactly=1)[0]
            for tr in table_body(f"{roman}.{part}", table, IA_HEADER):
                rows.append(parse_ia_row(roman, part, schema, tr))
            continue
        raise ParseError(f"{roman}: unexpected <{child.tag} class={cls!r}>")
    if not seen_subtitle:
        raise ParseError(f"{roman}: annex subtitle missing")
    if part_tables != [1 for _ in IA_PARTS]:
        raise ParseError(f"{roman}: part table counts {part_tables}")
    return rows


def check_registry() -> None:
    program = get_program_by_key(PROGRAM_KEY)
    if program is None:
        raise ParseError(f"unknown program key {PROGRAM_KEY!r}")
    if MEASURE not in get_args(Measure):
        raise ParseError(f"invalid measure {MEASURE!r}")
    if MEASURE not in program.measures:
        raise ParseError(f"measure {MEASURE!r} not in {PROGRAM_KEY}")
    for schema_name in [schema for _, schema in I_SECTIONS] + [
        schema for _, _, schema in IA_PARTS
    ]:
        if model.get(schema_name) is None:
            raise ParseError(f"unknown schema {schema_name!r}")


def parse_document(doc: Element) -> list[Row]:
    rows: list[Row] = []
    for roman, block in annex_blocks(doc, {"I", "Ia"} | NON_TARGET):
        if roman in NON_TARGET:
            continue
        annex_rows = (
            parse_annex_i(roman, block)
            if roman == "I"
            else parse_annex_ia(roman, block)
        )
        if not annex_rows:
            raise ParseError(f"{roman}: no entries extracted")
        rows.extend(annex_rows)
    return rows


@click.command(help="Parse consolidated Regulation 2022/2309 into a CSV candidate.")
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
            raise ParseError(f"not a consolidated 2022/2309 CELEX: {celex!r}")
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
