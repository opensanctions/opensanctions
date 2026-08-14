"""Parse consolidated Regulation (EC) 1210/2003 (Iraq) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation's annexes:

- Annex III — the Article 4(2) list of public bodies, corporations and
  agencies of the previous Iraqi government. Every entry but one has been
  delisted; the single live entry is transcribed in a reviewed table below,
  keyed on its number and holding the exact printed sentence next to its
  hand-reviewed column mapping.
- Annex IV — the Article 4(2), (3) and (4) list of persons and entities
  associated with the former Saddam Hussein regime, in two printed layouts:
  entries 1-55 use the UN narrative layout (a "N. NAME: …" heading, then
  uppercase-labelled lines, then a "UNSC RESOLUTION 1483 BASIS:" block whose
  bare lines are the reason); entries 56 and up are one-paragraph prose in
  the "Label: value" sentence style, sliced at sentence-boundary labels.
- Annexes I (petroleum goods), II (cultural goods) and V (competent
  authority websites) list no designations.

Neither annex prints per-designation listing dates, so `startDate` is empty
on every row. The combined "DATE OF BIRTH/PLACE OF BIRTH" label promises two
fields; its value is split at the first comma, with the date segment guarded
by the shapes actually observed. Kin-only "Other information" values
("daughter of …", "wife of …") and the two "Last known directors: …" lines
name other parties and are deliberately not transcribed; identifiers inside
other-information values go to their identifier columns instead. Trailing
sentence periods on sliced prose values are scaffolding and are stripped.

Output: data/consolidated/32003R1210.csv (the EU Journal consolidated CSV
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
    check_marker,
    clean,
    load_source,
    single_paragraph,
    split_values,
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

FRAMEWORK_CELEX = "32003R1210"
CONSOLIDATED_RE = re.compile(r"^02003R1210-\d{8}$")
PROGRAM_KEY = "EU-IRQ"
# Both designation annexes implement the Article 4 fund freezes; the other
# EU-IRQ measures (arms embargo, cultural-goods import restrictions) ride on
# the goods annexes and articles, not on these lists.
MEASURE = "Asset freeze"

NON_TARGET = frozenset({"I", "II", "V"})

III_SUBTITLE = (
    "List of public bodies, corporations and agencies and natural and legal "
    "persons, bodies and entities of the previous government of Iraq "
    "referred to in Article 4"
)
IV_SUBTITLE = (
    "List of natural and legal persons, bodies or entities associated with "
    "the regime of former President Saddam Hussein referred to in "
    "Article 4(2), (3) and (4)"
)

# The reviewed Annex III designations: entry number → the exact printed
# sentence, the schema, and the hand-reviewed column mapping. The sentence
# is compared against the document on every run; a mismatch (or any second
# live entry) is a re-review event.
III_ENTRIES: dict[str, tuple[str, str, tuple[tuple[str, str], ...]]] = {
    "53": (
        "IDLEB COMPANY FOR SPINNING. Address: P.O. Box 9, Idleb, Iraq",
        "LegalEntity",
        (
            ("name", "IDLEB COMPANY FOR SPINNING"),
            ("address", "P.O. Box 9, Idleb, Iraq"),
        ),
    ),
}

# --- Annex IV, entries 1-55 (UN narrative layout) ---------------------------

F1_HEADING_RE = re.compile(r"^(\d+)\. NAME: (.+)$")
F1_LABEL_RE = re.compile(r"^([A-Z][A-Z0-9 /']{1,40}):\s*(.*)$")
F1_ALIAS_LABEL = "ALIAS"
F1_DOB_POB_LABEL = "DATE OF BIRTH/PLACE OF BIRTH"
F1_NATIONALITY_LABEL = "NATIONALITY"
F1_BASIS_LABEL = "UNSC RESOLUTION 1483 BASIS"
# The date segment before the first comma, in exactly the observed shapes:
# a year, a worded date, or such atoms joined by " or ", optionally led by
# "Circa".
_DATE_ATOM = (
    r"(?:[Cc]irca )?(?:\d{1,2} (?:January|February|March|April|May|June|July"
    r"|August|September|October|November|December) )?\d{4}"
)
F1_DATE_RE = re.compile(rf"^{_DATE_ATOM}(?: or {_DATE_ATOM})*$")
# Labelled-looking lines printed inside a basis block, reviewed one by one;
# any other "LABEL:" line inside a basis is new structure.
BASIS_LINE_PINS: dict[str, tuple[tuple[str, str], ...]] = {
    "PASSPORT: (July 1997): No 34409/129": (
        ("passportNumber", "(July 1997): No 34409/129"),
    ),
    "NOTE: Died in 2003": (("notes", "Died in 2003"),),
}

# --- Annex IV, entries 56+ (labelled prose paragraphs) -----------------------

F2_ENTRY_RE = re.compile(r"^(\d+)\. (.+)$")
# Every reviewed prose entry's schema, pinned by number; a new number breaks
# for classification.
F2_SCHEMAS: dict[str, str] = {
    **{str(n): "Person" for n in (*range(56, 72), 73, 76, 81, 85)},
    **{str(n): "Person" for n in range(86, 92)},
    **{str(n): "LegalEntity" for n in (83, 84, 92, 93, 95, 96, 97, 100, 101)},
}
F2_COLUMNS = {
    "Date of birth": "birthDate",
    "Place of birth": "birthPlace",
    "Nationality": "nationality",
    "Address": "address",
    "Addresses": "address",
    "Passport No": "passportNumber",
    "Registered company number": "registrationNumber",
    "Registered office address": "address",
}
F2_OTHER_LABEL = "Other information"
F2_LABELS = (*F2_COLUMNS, F2_OTHER_LABEL)
# Deterministic repairs for printing defects in the prose entries, applied
# before slicing; each exact substring must occur exactly once.
MISPRINT_REPAIRS: dict[str, tuple[str, str]] = {
    # Comma where the sentence stop before the first label belongs.
    "62": ("Al-Tikriti, Date of birth:", "Al-Tikriti. Date of birth:"),
    # Missing sentence stop after the alias parenthetical.
    "60": ("(alias Chadian) Date of birth:", "(alias Chadian). Date of birth:"),
}
# Reviewed "Other information" values by entry: the exact printed value
# (after trailing-period stripping) and its column mapping. An empty
# mapping is a deliberate drop: kin-only prose ("daughter of …") and the
# "Last known directors" lines name other parties and have no column.
_KIN_BARZAN = "child of Barzan Ibrahim Hasan Al-Tikriti"
_KIN_SAJIDA = "daughter of Sajida Khayrallah Tilfah and Saddam Hussein"
_KIN_SABAWI = (
    "Son of Sabawi Ibrahim Hasan Al-Tikriti, former Presidential Advisor "
    "to Saddam Hussein"
)
_DIRECTORS = (
    "Last known directors: Hana Paul JON, Adnan Talib Hashim AL-AMIRI, "
    "Dr. Safa Hadi Jawad AL-HABOBI"
)
OTHER_INFO: dict[str, tuple[str, tuple[tuple[str, str], ...]]] = {
    "56": (
        "Saddam Hussein's officially recognised wife and mother of five of "
        "his children, including Qusay Saddam Hussein and Uday Saddam Hussein",
        (),
    ),
    "57": (_KIN_SAJIDA, ()),
    "58": (_KIN_SAJIDA, ()),
    "59": (_KIN_SAJIDA, ()),
    "60": ("Saddam Hussein's second wife and mother of his third son", ()),
    "61": ("son of Samira Shahbandar and Saddam Hussein", ()),
    "62": (_KIN_BARZAN, ()),
    "64": (_KIN_BARZAN, ()),
    "65": (_KIN_BARZAN, ()),
    "66": (_KIN_BARZAN, ()),
    "67": (_KIN_BARZAN, ()),
    "68": ("wife of Izzat Ibrahim Al-Duri", ()),
    "69": ("wife of Izzat Ibrahim Al-Duri", ()),
    "70": ("wife of Izzat Ibrahim Al-Duri", ()),
    "71": ("wife of Izzat Ibrahim Al-Duri", ()),
    "86": (_KIN_SABAWI, ()),
    "87": (_KIN_SABAWI, ()),
    "88": (_KIN_SABAWI, ()),
    "89": (_KIN_SABAWI, ()),
    "90": (_KIN_SABAWI, ()),
    "91": (_KIN_SABAWI, ()),
    "97": (
        "Federal No: CH-2 17-0-431-423-3 (Switzerland)",
        (("registrationNumber", "CH-2 17-0-431-423-3 (Switzerland)"),),
    ),
    "100": (_DIRECTORS, ()),
    "101": (_DIRECTORS, ()),
}
# Entries 62/68/69 print the birth place inside the "Date of birth" value;
# the exact value is split by review into its two contiguous spans.
F2_DOB_OVERRIDES: dict[str, tuple[str, tuple[tuple[str, str], ...]]] = {
    "68": (
        "circa 1942, Al-Dur, Iraq",
        (("birthDate", "circa 1942"), ("birthPlace", "Al-Dur, Iraq")),
    ),
    "69": (
        "Circa 1967, Kirkuk, Iraq",
        (("birthDate", "Circa 1967"), ("birthPlace", "Kirkuk, Iraq")),
    ),
}
# Entry 63 prints its kin statement as an unlabelled trailing sentence.
TRAILING_SENTENCE_PINS: dict[str, str] = {
    "63": "Child of Barzan Ibrahim Hasan Al-Tikriti",
}
ALIAS_TAIL_RE = re.compile(r"^(.+?) \(alias:? (.+)\)$")
# Entry 93's alias list omits the "(a)" marker on its first item; the
# reviewed group content maps to its items exactly.
ALIAS_GROUP_PINS: dict[str, tuple[str, tuple[str, ...]]] = {
    "93": (
        "AL-BASHAER TRADING COMPANY, LTD, (b) AL-BASHIR TRADING COMPANY, "
        "LTD, (c) AL-BASHA'IR TRADING COMPANY, LTD, (d) AL-BASHAAIR TRADING "
        "COMPANY, LTD, (e) AL-BUSHAIR TRADING COMPANY, LTD",
        (
            "AL-BASHAER TRADING COMPANY, LTD",
            "AL-BASHIR TRADING COMPANY, LTD",
            "AL-BASHA'IR TRADING COMPANY, LTD",
            "AL-BASHAAIR TRADING COMPANY, LTD",
            "AL-BUSHAIR TRADING COMPANY, LTD",
        ),
    ),
}


def check_registry() -> None:
    program = get_program_by_key(PROGRAM_KEY)
    if program is None:
        raise ParseError(f"unknown program key {PROGRAM_KEY!r}")
    if MEASURE not in get_args(Measure):
        raise ParseError(f"invalid measure {MEASURE!r}")
    if MEASURE not in program.measures:
        raise ParseError(f"measure {MEASURE!r} not in {PROGRAM_KEY}")
    schemata = {"Person", *F2_SCHEMAS.values()}
    schemata.update(schema for _, schema, _ in III_ENTRIES.values())
    for schema_name in schemata:
        if model.get(schema_name) is None:
            raise ParseError(f"unknown schema {schema_name!r}")


def check_subtitle(ctx: str, child: Element, expected: str, seen: bool) -> None:
    if seen:
        raise ParseError(f"{ctx}: second annex subtitle")
    if clean(element_text(child), ctx) != expected:
        raise ParseError(f"{ctx}: annex subtitle changed")


def strip_period(value: str) -> str:
    return value[:-1].strip() if value.endswith(".") else value


def split_lettered(ctx: str, value: str) -> list[str]:
    """Split a printed "(a) …, (b) …" enumeration in sequence-checked order."""
    positions: list[int] = []
    for index in range(26):
        marker = f"({chr(ord('a') + index)})"
        at = value.find(marker)
        if at < 0:
            break
        if positions and at < positions[-1]:
            raise ParseError(f"{ctx}: lettered items out of sequence")
        positions.append(at)
    if not positions or positions[0] != 0:
        raise ParseError(f"{ctx}: enumeration does not start with (a)")
    items: list[str] = []
    for index, start in enumerate(positions):
        end = positions[index + 1] if index + 1 < len(positions) else len(value)
        item = value[start + 3 : end].strip().rstrip(",;").strip()
        if not item:
            raise ParseError(f"{ctx}: empty lettered item")
        items.append(item)
    return items


# --- Annex III ---------------------------------------------------------------


def parse_annex_iii(annex: str, block: Element) -> list[Row]:
    rows: list[Row] = []
    seen_subtitle = False
    for child in block.iterchildren():
        if not isinstance(child.tag, str):
            continue
        cls = child.get("class") or ""
        if child.tag == "hr" and cls == "separator-annex":
            continue
        if child.tag == "p" and cls == "modref":
            check_marker(" ".join(element_text(child).split()), annex)
            continue
        if child.tag == "p" and cls == "title-annex-2":
            check_subtitle(annex, child, III_SUBTITLE, seen_subtitle)
            seen_subtitle = True
            continue
        if child.tag == "p" and cls in SKIP_P_CLASSES:
            if cls == "" and clean(element_text(child), annex):
                raise ParseError(f"{annex}: unexpected paragraph content")
            continue
        if child.tag == "div" and cls == "grid-container grid-list":
            rows.append(parse_iii_entry(annex, child))
            continue
        raise ParseError(f"{annex}: unexpected <{child.tag} class={cls!r}>")
    if not seen_subtitle:
        raise ParseError(f"{annex}: missing annex subtitle")
    return rows


def parse_iii_entry(annex: str, grid: Element) -> Row:
    columns = xpath_elements(grid, "./div")
    if len(columns) != 2:
        raise ParseError(f"{annex}: entry grid has {len(columns)} columns")
    number = clean(element_text(columns[0]), annex)
    match = re.match(r"^(\d+)\.$", number)
    if match is None:
        raise ParseError(f"{annex}: unrecognized entry number {number!r}")
    record_id = match.group(1)
    ctx = f"{annex} entry {record_id}"
    reviewed = III_ENTRIES.get(record_id)
    if reviewed is None:
        raise ParseError(f"{ctx}: entry not in the reviewed table")
    expected, schema, mapped = reviewed
    text = single_paragraph(columns[1], ctx)
    if text != expected:
        raise ParseError(f"{ctx}: printed text changed")
    row = Row(annex, schema, MEASURE, record_id=record_id)
    for column, value in mapped:
        row.add(column, [value])
    return row


# --- Annex IV, narrative entries 1-55 ----------------------------------------


def split_dob_pob(ctx: str, value: str, row: Row) -> None:
    date, sep, place = value.partition(", ")
    if not sep or F1_DATE_RE.match(date) is None:
        raise ParseError(f"{ctx}: unrecognized date/place value {value[:60]!r}")
    row.add("birthDate", [date])
    row.add("birthPlace", [place])


class _Narrative:
    """Accumulates one 1-55 entry across its heading and labelled lines."""

    def __init__(self, ctx: str, row: Row) -> None:
        self.ctx = ctx
        self.row = row
        self.block = ""
        self.reason_parts: list[str] = []

    def feed(self, line: str) -> None:
        labelled = F1_LABEL_RE.match(line)
        if labelled is not None and line in BASIS_LINE_PINS:
            if self.block != "basis":
                raise ParseError(f"{self.ctx}: pinned line outside basis block")
            for column, value in BASIS_LINE_PINS[line]:
                self.row.add(column, [value])
            return
        if labelled is not None:
            label, value = labelled.group(1), labelled.group(2)
            if label == F1_ALIAS_LABEL:
                self.row.add("alias", split_values(value))
                self.block = "alias"
                return
            if label == F1_DOB_POB_LABEL:
                split_dob_pob(self.ctx, value, self.row)
                self.block = ""
                return
            if label == F1_NATIONALITY_LABEL:
                self.row.add("nationality", [value])
                self.block = ""
                return
            if label == F1_BASIS_LABEL:
                if value != "" or self.block == "basis":
                    raise ParseError(f"{self.ctx}: malformed basis label")
                self.block = "basis"
                return
            raise ParseError(f"{self.ctx}: unrecognized label {label!r}")
        if self.block == "alias":
            self.row.add("alias", [line])
            return
        if self.block == "basis":
            self.reason_parts.append(line)
            return
        raise ParseError(f"{self.ctx}: unrecognized line {line[:60]!r}")

    def finish(self) -> Row:
        if not self.reason_parts:
            raise ParseError(f"{self.ctx}: entry has no basis block")
        self.row.reason = " ".join(self.reason_parts)
        return self.row


# --- Annex IV, prose entries 56+ ----------------------------------------------


def slice_prose(ctx: str, text: str) -> tuple[str, list[tuple[str, str]]]:
    """Split a prose entry into its head and ". Label: value" fields."""
    cuts: list[tuple[int, str]] = []
    for label in F2_LABELS:
        for match in re.finditer(re.escape(f". {label}: "), text):
            cuts.append((match.start(), label))
    cuts.sort()
    if not cuts:
        raise ParseError(f"{ctx}: no labelled fields in prose entry")
    head = text[: cuts[0][0]]
    fields: list[tuple[str, str]] = []
    for index, (start, label) in enumerate(cuts):
        end = cuts[index + 1][0] if index + 1 < len(cuts) else len(text)
        value = text[start + len(f". {label}: ") : end]
        fields.append((label, strip_period(value.strip())))
    return head, fields


def parse_prose_name(ctx: str, record_id: str, head: str, row: Row) -> None:
    tail = ALIAS_TAIL_RE.match(head)
    if tail is not None:
        head, group = tail.groups()
        pinned = ALIAS_GROUP_PINS.get(record_id)
        if pinned is not None:
            expected, aliases = pinned
            if group != expected:
                raise ParseError(f"{ctx}: pinned alias group changed")
            row.add("alias", list(aliases))
        elif group.startswith("(a)"):
            row.add("alias", split_lettered(ctx, group))
        elif "(b)" in group:
            raise ParseError(f"{ctx}: unrecognized alias group {group[:60]!r}")
        else:
            row.add("alias", [group])
    if "(alias" in head:
        raise ParseError(f"{ctx}: unextracted alias in name {head[:60]!r}")
    # The head never carries a sentence stop (the label cut or the alias
    # parenthetical consumes it); a trailing period is part of the printed
    # name ("LOGARCHEO S.A.").
    row.add("name", [head])


def parse_prose_entry(annex: str, text: str) -> Row:
    match = F2_ENTRY_RE.match(text)
    if match is None:
        raise ParseError(f"{annex}: unrecognized prose entry {text[:60]!r}")
    record_id, body = match.groups()
    ctx = f"{annex} entry {record_id}"
    schema = F2_SCHEMAS.get(record_id)
    if schema is None:
        raise ParseError(f"{ctx}: entry has no reviewed schema pin")
    repair = MISPRINT_REPAIRS.get(record_id)
    if repair is not None:
        broken, fixed = repair
        if body.count(broken) != 1:
            raise ParseError(f"{ctx}: pinned misprint not found")
        body = body.replace(broken, fixed)
    trailing = TRAILING_SENTENCE_PINS.get(record_id)
    if trailing is not None:
        suffix = f". {trailing}"
        if not body.endswith(suffix):
            raise ParseError(f"{ctx}: pinned trailing sentence not found")
        body = body[: -len(suffix)]
    row = Row(annex, schema, MEASURE, record_id=record_id)
    head, fields = slice_prose(ctx, body)
    parse_prose_name(ctx, record_id, head, row)
    for label, value in fields:
        if value == "":
            raise ParseError(f"{ctx}: empty value for label {label!r}")
        if label == F2_OTHER_LABEL:
            reviewed = OTHER_INFO.get(record_id)
            if reviewed is None or reviewed[0] != value:
                raise ParseError(f"{ctx}: unreviewed other-information value")
            for column, mapped in reviewed[1]:
                row.add(column, [mapped])
            continue
        if label == "Date of birth" and record_id in F2_DOB_OVERRIDES:
            expected, mapped_pairs = F2_DOB_OVERRIDES[record_id]
            if value != expected:
                raise ParseError(f"{ctx}: pinned date value changed")
            for column, mapped in mapped_pairs:
                row.add(column, [mapped])
            continue
        column = F2_COLUMNS[label]
        if value.startswith("(a)"):
            row.add(column, split_lettered(ctx, value))
        else:
            row.add(column, [value])
    return row


# --- Annex IV walk ------------------------------------------------------------


def parse_annex_iv(annex: str, block: Element) -> list[Row]:
    rows: list[Row] = []
    seen_subtitle = False
    entry: _Narrative | None = None

    def close_entry() -> None:
        nonlocal entry
        if entry is not None:
            rows.append(entry.finish())
            entry = None

    for child in block.iterchildren():
        if not isinstance(child.tag, str):
            continue
        cls = child.get("class") or ""
        if child.tag == "hr" and cls == "separator-annex":
            continue
        if child.tag == "p" and cls == "modref":
            check_marker(" ".join(element_text(child).split()), annex)
            continue
        if child.tag == "p" and cls == "title-annex-2":
            check_subtitle(annex, child, IV_SUBTITLE, seen_subtitle)
            seen_subtitle = True
            continue
        if child.tag == "p" and cls == "title-gr-seq-level-1":
            close_entry()
            heading = clean(element_text(child), annex)
            match = F1_HEADING_RE.match(heading)
            if match is None:
                raise ParseError(f"{annex}: unrecognized heading {heading[:60]!r}")
            record_id, name = match.groups()
            if int(record_id) > 55:
                raise ParseError(f"{annex}: narrative entry beyond 55")
            ctx = f"{annex} entry {record_id}"
            row = Row(annex, "Person", MEASURE, record_id=record_id)
            row.add("name", [name])
            entry = _Narrative(ctx, row)
            continue
        if child.tag == "p" and cls == "norm":
            if entry is None:
                raise ParseError(f"{annex}: stray paragraph outside an entry")
            entry.feed(clean(element_text(child), annex))
            continue
        if child.tag == "p" and cls in SKIP_P_CLASSES:
            if cls == "" and clean(element_text(child), annex):
                raise ParseError(f"{annex}: unexpected paragraph content")
            continue
        if child.tag == "div" and cls == "":
            close_entry()
            ctx = f"{annex} prose entry"
            rows.append(parse_prose_entry(annex, single_paragraph(child, ctx)))
            continue
        raise ParseError(f"{annex}: unexpected <{child.tag} class={cls!r}>")
    close_entry()
    if not seen_subtitle:
        raise ParseError(f"{annex}: missing annex subtitle")
    numbers = [int(row.record_id) for row in rows]
    if numbers != sorted(set(numbers)):
        raise ParseError(f"{annex}: entry numbers not strictly increasing")
    return rows


def parse_document(doc: Element) -> list[Row]:
    rows: list[Row] = []
    for annex, block in annex_blocks(doc, {"III", "IV"} | NON_TARGET):
        if annex in NON_TARGET:
            continue
        parse = parse_annex_iii if annex == "III" else parse_annex_iv
        annex_rows = parse(annex, block)
        if not annex_rows:
            raise ParseError(f"{annex}: no entries extracted")
        rows.extend(annex_rows)
    return rows


@click.command(help="Parse consolidated Regulation 1210/2003 into a CSV candidate.")
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
            raise ParseError(f"not a consolidated 1210/2003 CELEX: {celex!r}")
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
