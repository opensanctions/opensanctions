"""Parse consolidated Regulation (EC) 1183/2005 (DR Congo) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation's target annexes:

- Annex I — the UN DRC list (UNSCR 1533 committee, Articles 2 and 2a), two
  grid-list parts lettered a) persons and b) entities, each a line-oriented
  UN layout: a numbered name heading (optionally carrying an "(alias: …)"
  or "(low quality a.k.a.: …)" parenthetical), an optional alias
  parenthetical line, labelled field lines, an "Other information" line
  whose bare follow-on paragraphs continue it as notes, and — where the
  entry prints the "Additional information from the narrative summary …"
  sentinel — narrative paragraphs that form the reason. Entries added since
  2024 print their listing basis inside "Other information" and have no
  sentinel; their reason is empty and the basis prose stays in notes,
  exactly as labelled. "Reason for listing:" / "Additional information:"
  sub-headings inside the narrative are structural markers: standalone
  lines are skipped and paragraph prefixes stripped. Part b) mixes armed
  groups with registered companies under the one "entities" heading; all
  are emitted as LegalEntity. The printed part letters are the annex
  identifiers (I.a, I.b).
- Annex Ia — the EU-autonomous Article 2b list, five-column tables in parts
  A. Persons (Person) and B. Entities (LegalEntity).

Annex II lists competent-authority websites, not designations. Travel bans
live in Decision 2010/788/CFSP. Dates are transcribed as the source prints
them ("31 December 2012", "1 Nov. 2005", "12.12.2016"); the crawler
normalizes dates.

Output: data/consolidated/32005R1183.csv (the EU Journal consolidated CSV
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

FRAMEWORK_CELEX = "32005R1183"
CONSOLIDATED_RE = re.compile(r"^02005R1183-\d{8}$")
PROGRAM_KEY = "EU-COD"
# Annexes I and Ia implement the Articles 2/2a/2b fund freeze; travel bans
# live in Decision 2010/788/CFSP.
MEASURE = "Asset freeze"

NON_TARGET = frozenset({"II"})

# --- Annex I (UN list, grid-list parts) ---------------------------------------

# (printed part letter, printed part subtitle, part id, schema) in print
# order. Part b) mixes armed groups and registered companies under the one
# "entities" heading; LegalEntity is the schema that covers both.
I_PARTS = (
    (
        "a)",
        "List of persons referred to in Articles 2 and 2a.",
        "a",
        "Person",
    ),
    (
        "b)",
        "List of entities referred to in Articles 2 and 2a.",
        "b",
        "LegalEntity",
    ),
)

I_HEADING_RE = re.compile(r"^(\d+)\. (.+)$")
# A heading may end in an alias parenthetical; unlabeled parentheticals
# ("ADF (ALLIED DEMOCRATIC FORCES)") are part of the printed name and stay.
I_HEADING_ALIAS_RE = re.compile(
    r"^(.+?) \((alias|low quality a\.k\.a\.)\s*:?\s*(.+)\)$"
)
# Alias parenthetical lines right after the heading, in the printed
# spellings "(alias: …)", "(Alias: …)", "(alias a) …)" and — entry a4's
# misprint — "(alias a): …)"; one line prints a trailing period after the
# closing paren.
I_ALIAS_LINE_RE = re.compile(r"^\((?:alias|Alias)\s*:?\s*(.+)\)\.?$")

# Labelled field lines → CSV column, as printed. Lettered "a) … b) …"
# values split into one value per item.
I_FIELD_COLUMNS = {
    "Designation": "position",
    "Title": "position",
    "Date of Birth": "birthDate",
    "Date of birth": "birthDate",
    "Place of Birth": "birthPlace",
    "Place of birth": "birthPlace",
    "Nationality": "nationality",
    "Address": "address",
    "Passport number": "passportNumber",
    "National identification no": "idNumber",
}
I_DATE_LABEL = "Date of UN designation"
# One date prints an amendment-history parenthetical, stripped per the
# contract's startDate rule.
I_DATE_AMENDED_RE = re.compile(r"^(.+?) \(amended on .+\)$")
I_NOTES_LABELS = frozenset({"Other information", "Other Information"})

# Everything after this line is narrative reason.
I_SENTINEL = (
    "Additional information from the narrative summary of reasons for "
    "listing provided by the Sanctions Committee:"
)
# Structural sub-headings inside the narrative: standalone lines are
# skipped, paragraph prefixes stripped.
I_SUBHEADS = (
    "Reason for listing:",
    "Additional Information:",
    "Additional information:",
)

# Source misprints repaired before parsing, as exact printed → repaired
# pairs; a changed line breaks for re-review. Entry a12's alias line lost
# its closing parenthesis.
I_MISPRINT_REPAIRS = {
    "(alias: a) Mupenzi Bernard, b) General Major Mupenzi, c) General"
    " Mudacumura, d) Pharaoh, e) Radja": (
        "(alias: a) Mupenzi Bernard, b) General Major Mupenzi, c) General"
        " Mudacumura, d) Pharaoh, e) Radja)"
    ),
}

# Reviewed decompositions for "Other information" values that embed
# structured identifiers, keyed by (part, entry) with the exact printed
# value; a changed value breaks for re-review.
I_OI_OVERRIDES: dict[tuple[str, str], tuple[str, tuple[tuple[str, str], ...]]] = {
    ("b", "5"): (
        "Email: Fdlr@fmx.de; fldrrse@yahoo.fr; fdlr@gmx.net; fdlrsrt@gmail.com;"
        " humura2020@gmail.com. INTERPOL-UN Security Council Special Notice web"
        " link: https://www.interpol.int/en/notice/search/une/5278442",
        (
            ("email", "Fdlr@fmx.de"),
            ("email", "fldrrse@yahoo.fr"),
            ("email", "fdlr@gmx.net"),
            ("email", "fdlrsrt@gmail.com"),
            ("email", "humura2020@gmail.com"),
            (
                "notes",
                "INTERPOL-UN Security Council Special Notice web link:"
                " https://www.interpol.int/en/notice/search/une/5278442",
            ),
        ),
    ),
    ("b", "6"): (
        "Email: mouvementdu23mars1@gmail.com. INTERPOL-UN Security Council"
        " Special Notice web link:"
        " https://www.interpol.int/en/notice/search/une/5277973",
        (
            ("email", "mouvementdu23mars1@gmail.com"),
            (
                "notes",
                "INTERPOL-UN Security Council Special Notice web link:"
                " https://www.interpol.int/en/notice/search/une/5277973",
            ),
        ),
    ),
}

# --- Annex Ia (EU-autonomous tables) ------------------------------------------

# (printed part heading, part id, schema) in print order.
IA_PARTS = (
    ("A. Persons", "A", "Person"),
    ("B. Entities", "B", "LegalEntity"),
)
IA_HEADER = (
    "",
    "Name",
    "Identifying information",
    "Reasons",
    "Date of listing",
)
IA_NUMBER_RE = re.compile(r"^(\d+)\.$")
IA_AKA_RE = re.compile(r"^a\.k\.a\.\s*(.*)$")
IA_FIELD_COLUMNS = {
    "Function/rank": "position",
    "Function or profession": "position",
    "Position": "position",
    "Rank": "position",
    "DOB": "birthDate",
    "POB": "birthPlace",
    "Place of birth": "birthPlace",
    "Nationality": "nationality",
    "Gender": "gender",
    "Address": "address",
    "DRC passport number": "passportNumber",
    "Passport number": "passportNumber",
    "Military ID number": "idNumber",
    "Military ID": "idNumber",
    "Military service number": "idNumber",
    "RDF Service number": "idNumber",
    "Schengen visa number": "idNumber",
    "Father": "fatherName",
    "Date of creation": "incorporationDate",
    "Place of creation": "address",
    "Place of establishment": "address",
}
# Relational labels naming other parties: no CSV column, deliberately not
# transcribed.
IA_DROP_LABELS = frozenset({"Associated entities"})
# The one printed placeholder value ("DOB: unknown", entry A23).
IA_PLACEHOLDER = "unknown"
# Entry A37 prints a second role as a bare line under Function/rank; any
# other unlabeled info line raises.
IA_ROLE_LINE_PINS = frozenset({("A", "37")})


def verbatim_date(text: str, ctx: str) -> str:
    # Formats observed in this document: worded ("31 December 2012") and
    # UN-abbreviated ("1 Nov. 2005") dates in Annex I, dotted dates
    # ("12.12.2016") in Annex Ia. The printed wording is kept; the
    # recognizers only guard the shape.
    for parse in (parse_worded_date, parse_abbrev_date, parse_dotted_date):
        if parse(text) is not None:
            return text
    raise ParseError(f"{ctx}: unrecognized date {text!r}")


def unwrap_quotes(value: str) -> str:
    if value.startswith("‘") and value.endswith("’"):
        return value[1:-1]
    return value


# Lettered markers in alias parentheticals and field values: "a) ", "(a) ",
# and — entry a4's misprint — "a): ".
I_LETTER_RE = re.compile(r"(?:(?<=^)|(?<=[ ,;(]))\(?([a-z])\):?\s+")


def split_lettered(ctx: str, value: str) -> list[str]:
    """Split a lettered "a) … b) …" list, checking the letters run in order.

    A value without lettered markers is one item. Items keep their internal
    punctuation; the trailing list separator ("," or ";") is stripped.
    """
    matches = list(I_LETTER_RE.finditer(value))
    if not matches:
        return [value]
    if matches[0].start() != 0:
        raise ParseError(f"{ctx}: lettered list has a headless item {value[:60]!r}")
    letters = [match.group(1) for match in matches]
    expected = [chr(ord("a") + i) for i in range(len(letters))]
    if letters != expected:
        raise ParseError(f"{ctx}: lettered markers {letters} out of sequence")
    items: list[str] = []
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(value)
        item = value[match.end() : end].strip().rstrip(",;").strip()
        # The misprint marker "b):" leaves its "(" behind on "(a) …" lists.
        item = item.rstrip("(").strip()
        if not item:
            raise ParseError(f"{ctx}: empty lettered item in {value[:60]!r}")
        items.append(item)
    return items


def split_aliases(ctx: str, value: str) -> list[str]:
    items: list[str] = []
    for item in split_lettered(ctx, value):
        for piece in item.split(";"):
            piece = piece.strip()
            if piece:
                items.append(unwrap_quotes(piece))
    if not items:
        raise ParseError(f"{ctx}: empty alias list")
    return items


def strip_sentence_period(value: str) -> str:
    return value[:-1].strip() if value.endswith(".") else value


def parse_i_heading(ctx: str, part: str, schema: str, text: str) -> Row:
    match = I_HEADING_RE.match(text)
    if match is None:
        raise ParseError(f"{ctx}: unrecognized entry heading {text[:60]!r}")
    record_id, body = match.groups()
    row = Row(annex_id("I", part), schema, MEASURE, record_id=record_id)
    alias_match = I_HEADING_ALIAS_RE.match(body)
    if alias_match is not None:
        body, label, alias_group = alias_match.groups()
        column = "weakAlias" if label == "low quality a.k.a." else "alias"
        row.add(column, split_aliases(f"{ctx} entry {record_id}", alias_group))
    if "alias" in body or "a.k.a" in body:
        raise ParseError(f"{ctx}: unextracted alias in name {body[:60]!r}")
    row.add("name", [body])
    return row


def apply_i_field(ctx: str, row: Row, label: str, value: str) -> None:
    value = strip_sentence_period(value)
    if not value:
        raise ParseError(f"{ctx}: empty value for label {label!r}")
    if label == I_DATE_LABEL:
        amended = I_DATE_AMENDED_RE.match(value)
        if amended is not None:
            value = amended.group(1)
        row.start_date = verbatim_date(value, ctx)
        return
    row.add(I_FIELD_COLUMNS[label], split_lettered(ctx, value))


def finish_i_entry(
    ctx: str, row: Row, narrative: list[str], saw_sentinel: bool
) -> None:
    if not row.start_date:
        raise ParseError(f"{ctx}: entry has no UN designation date")
    if saw_sentinel and not narrative:
        raise ParseError(f"{ctx}: sentinel without narrative")
    row.reason = " ".join(narrative)


def parse_i_part(part: str, schema: str, subtitle: str, col: Element) -> list[Row]:
    ctx = f"I.{part}"
    rows: list[Row] = []
    row: Row | None = None
    narrative: list[str] = []
    # States per entry: fields → notes (after Other information) → reason
    # (after the sentinel).
    state = ""
    seen_subtitle = False

    def close_entry() -> None:
        nonlocal row
        if row is not None:
            finish_i_entry(
                f"{ctx} entry {row.record_id}", row, narrative, state == "reason"
            )
            rows.append(row)
            row = None

    for child in col.iterchildren():
        if not isinstance(child.tag, str):
            continue
        cls = child.get("class") or ""
        if child.tag != "p":
            raise ParseError(f"{ctx}: unexpected <{child.tag} class={cls!r}>")
        if cls == "modref":
            check_marker(" ".join(element_text(child).split()), ctx)
            continue
        if cls in SKIP_P_CLASSES:
            continue
        text = clean(element_text(child), ctx)
        if not seen_subtitle:
            if cls != "norm" or text != subtitle:
                raise ParseError(f"{ctx}: unexpected part subtitle {text[:60]!r}")
            seen_subtitle = True
            continue
        if cls == "title-gr-seq-level-1":
            close_entry()
            row = parse_i_heading(ctx, part, schema, text)
            if rows and int(row.record_id) != int(rows[-1].record_id) + 1:
                raise ParseError(f"{ctx}: entry {row.record_id} out of sequence")
            narrative, state = [], "fields"
            continue
        if row is None:
            raise ParseError(f"{ctx}: text outside any entry: {text[:50]!r}")
        entry_ctx = f"{ctx} entry {row.record_id}"
        if cls == "title-gr-seq-level-2":
            if text != I_SENTINEL or state == "reason":
                raise ParseError(f"{entry_ctx}: unexpected sub-heading {text[:50]!r}")
            state = "reason"
            continue
        if cls == "title-gr-seq-level-3":
            if text not in I_SUBHEADS or state != "reason":
                raise ParseError(f"{entry_ctx}: unexpected sub-heading {text[:50]!r}")
            continue
        if cls not in ("norm", "list"):
            raise ParseError(f"{entry_ctx}: unexpected <p class={cls!r}>")
        text = I_MISPRINT_REPAIRS.get(text, text)
        if text == I_SENTINEL:
            if state == "reason":
                raise ParseError(f"{entry_ctx}: second sentinel")
            state = "reason"
            continue
        if state == "reason":
            if text in I_SUBHEADS:
                continue
            for subhead in I_SUBHEADS:
                if text.startswith(subhead + " "):
                    text = text[len(subhead) + 1 :]
                    break
            narrative.append(text)
            continue
        if state == "notes":
            # Bare paragraphs continue the Other information run.
            row.add("notes", [text])
            continue
        alias_line = I_ALIAS_LINE_RE.match(text)
        if alias_line is not None:
            if state != "fields" or row.props.get("nationality") or row.start_date:
                raise ParseError(f"{entry_ctx}: alias line after fields")
            row.add("alias", split_aliases(entry_ctx, alias_line.group(1)))
            continue
        labelled = LABELLED_RE.match(text)
        if labelled is not None and labelled.group(1) in I_NOTES_LABELS:
            value = labelled.group(2)
            override = I_OI_OVERRIDES.get((part, row.record_id))
            if override is not None:
                expected, mapped = override
                if value != expected:
                    raise ParseError(f"{entry_ctx}: override value changed")
                for column, mapped_value in mapped:
                    row.add(column, [mapped_value])
            elif value:
                row.add("notes", [value])
            else:
                raise ParseError(f"{entry_ctx}: empty Other information")
            state = "notes"
            continue
        if labelled is not None and (
            labelled.group(1) in I_FIELD_COLUMNS or labelled.group(1) == I_DATE_LABEL
        ):
            apply_i_field(entry_ctx, row, labelled.group(1), labelled.group(2))
            continue
        raise ParseError(f"{entry_ctx}: unrecognized field line {text[:60]!r}")
    close_entry()
    if not seen_subtitle:
        raise ParseError(f"{ctx}: part subtitle missing")
    return rows


def parse_annex_i(roman: str, block: Element) -> list[Row]:
    rows: list[Row] = []
    part_index = -1
    for child in block.iterchildren():
        if not isinstance(child.tag, str):
            continue
        cls = child.get("class") or ""
        if child.tag == "hr" and cls == "separator-annex":
            continue
        if child.tag == "p" and cls in SKIP_P_CLASSES:
            continue
        if child.tag == "div" and "grid-container" in cls:
            part_index += 1
            if part_index >= len(I_PARTS):
                raise ParseError(f"{roman}: more grid parts than expected")
            letter, subtitle, part, schema = I_PARTS[part_index]
            col1, col2 = xpath_elements(child, "./div", expect_exactly=2)
            printed = clean(element_text(col1), roman)
            if printed != letter:
                raise ParseError(f"{roman}: unexpected part letter {printed!r}")
            rows.extend(parse_i_part(part, schema, subtitle, col2))
            continue
        raise ParseError(f"{roman}: unexpected <{child.tag} class={cls!r}>")
    if part_index + 1 != len(I_PARTS):
        raise ParseError(f"{roman}: saw {part_index + 1} grid parts")
    return rows


# --- Annex Ia ----------------------------------------------------------------


def parse_ia_info(ctx: str, part: str, record_id: str, td: Element, row: Row) -> None:
    # States: an empty-valued "a.k.a." opens a block whose next line holds
    # the alias values; a drop label consumes nothing further (single-line
    # values only in this document).
    aka_block = False
    for line in cell_lines(td, ctx):
        if aka_block:
            row.add("alias", [unwrap_quotes(item) for item in split_values(line)])
            aka_block = False
            continue
        aka = IA_AKA_RE.match(line)
        if aka is not None:
            if aka.group(1):
                row.add(
                    "alias",
                    [unwrap_quotes(item) for item in split_values(aka.group(1))],
                )
            else:
                aka_block = True
            continue
        labelled = LABELLED_RE.match(line)
        label = labelled.group(1) if labelled is not None else None
        if label in IA_DROP_LABELS:
            continue
        if labelled is not None and label in IA_FIELD_COLUMNS:
            value = labelled.group(2)
            if not value:
                raise ParseError(f"{ctx}: empty value for {label!r}")
            if value == IA_PLACEHOLDER:
                continue
            row.add(IA_FIELD_COLUMNS[label], split_values(value))
            continue
        if (part, record_id) in IA_ROLE_LINE_PINS:
            # A second role printed as a bare line under Function/rank.
            row.add("position", [line])
            continue
        raise ParseError(f"{ctx}: unrecognized info line {line[:60]!r}")
    if aka_block:
        raise ParseError(f"{ctx}: a.k.a. block without values")


def parse_ia_row(roman: str, part: str, schema: str, tr: Element) -> Row:
    cells = xpath_elements(tr, "./td|./th")
    ctx = f"{roman}.{part}"
    number = IA_NUMBER_RE.match(cell_line(cells[0], ctx))
    if number is None:
        raise ParseError(f"{ctx}: unrecognized entry number cell")
    record_id = number.group(1)
    ctx = f"{ctx} entry {record_id}"
    row = Row(annex_id(roman, part), schema, MEASURE, record_id=record_id)
    row.add("name", [cell_line(cells[1], ctx)])
    parse_ia_info(ctx, part, record_id, cells[2], row)
    row.reason = " ".join(cell_lines(cells[3], ctx))
    row.start_date = verbatim_date(cell_line(cells[4], ctx), ctx)
    return row


def parse_annex_ia(roman: str, block: Element) -> list[Row]:
    rows: list[Row] = []
    part_index = -1
    part_tables = [0 for _ in IA_PARTS]
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
            previous = 0
            for tr in table_body(f"{roman}.{part}", table, IA_HEADER):
                row = parse_ia_row(roman, part, schema, tr)
                if int(row.record_id) <= previous:
                    raise ParseError(
                        f"{roman}.{part}: entry {row.record_id} out of sequence"
                    )
                previous = int(row.record_id)
                rows.append(row)
            continue
        raise ParseError(f"{roman}: unexpected <{child.tag} class={cls!r}>")
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
    for schema_name in [schema for _, _, _, schema in I_PARTS] + [
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


@click.command(help="Parse consolidated Regulation 1183/2005 into a CSV candidate.")
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
            raise ParseError(f"not a consolidated 1183/2005 CELEX: {celex!r}")
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
