"""Parse consolidated Regulation (EU) 2024/2642 (Russia hybrid threats) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation's annexes:

- Annex I — the Article 2 fund-freeze list, with parts A. Natural persons
  and B. Legal persons, entities and bodies, each printed as one five-column
  table (entry number, name, identifying information, statement of reasons,
  date of listing). Travel bans live in Decision (CFSP) 2024/2643, not in
  this regulation.
- Annex II — competent-authority websites, not designations.
- Annexes III (tangible assets, Article 1a), IV and V (legal persons,
  Articles 1b and 1c) are reserved lists that currently print only a "[…]"
  placeholder. The parser accepts exactly that placeholder and breaks when
  the Council first populates them, so the new list shape gets reviewed.

The name cell holds the Latin-script name on its first line; further lines
are native-script renderings in parentheses ("(Russian: …)"), labelled alias
blocks ("a.k.a.", "Alias"), or unlabeled variant renderings — one rendering
per printed line, all transcribed as aliases. Early entries print the
person's function as unlabeled lines at the top of the identifying
information cell, before any labelled line. Relational lines naming other
parties ("Founder: …") have no CSV column and are deliberately not
transcribed. Dates are transcribed as the source prints them ("16.12.2024");
the crawler normalizes dates.

Output: data/consolidated/32024R2642.csv (the EU Journal consolidated CSV
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

FRAMEWORK_CELEX = "32024R2642"
CONSOLIDATED_RE = re.compile(r"^02024R2642-\d{8}$")
PROGRAM_KEY = "EU-RUSDA"
# Annex I implements the regulation's Article 2 fund freeze; travel bans
# live in Decision (CFSP) 2024/2643.
MEASURE = "Asset freeze"

NON_TARGET = frozenset({"II"})
# Reserved lists that print only a "[…]" placeholder in this version. Once
# the Council populates one, the placeholder check fails and the new list
# structure is taught here (Annex III holds tangible assets — vessels or
# aircraft — which map to different CSV schemata than legal persons).
PLACEHOLDER_ANNEXES = ("III", "IV", "V")
PLACEHOLDER = "[…]"

# Annex I prints each part as a grid-list: the letter in column 1, the part
# heading and one entry table in column 2.
PART_HEADER = (
    "",
    "Name",
    "Identifying information",
    "Statement of Reasons",
    "Date of listing",
)
# (letter cell, part heading, part id, schema) in print order.
PARTS = (
    ("A.", "Natural persons", "A", "Person"),
    ("B.", "Legal persons, entities and bodies", "B", "LegalEntity"),
)
# Entries whose printed identifiers (KPP codes) only exist on the Company
# schema; the source states their corporate form ("Joint Stock Company",
# a Federal State Unitary Enterprise).
COMPANY_PINS = frozenset({("B", "6"), ("B", "8")})

NUMBER_RE = re.compile(r"^(\d+)\.$")
# Alias labels in the name cell: bare block openers with the aliases on the
# following lines ("a.k.a.", "A.k.a.:", "Alias", "Alias:") and inline forms
# ("a.k.a. Andrey PETROV", "A.k.a.: Sasha JOST").
ALIAS_HEAD_RE = re.compile(r"^(?:a\.k\.a\.|A\.k\.a\.:|Alias:?)$")
ALIAS_LINE_RE = re.compile(r"^(?:a\.k\.a\.|A\.k\.a\.:) (.+)$")
# A full-line parenthetical is a native-script rendering, optionally
# annotated with its language ("(Russian: Артём Сергеевич КУРЕЕВ)", bare
# "(Виса Нохаевич МИЗАЕВ)"). One parenthetical can carry several renderings
# separated by ";", each with its own language label ("(Ukrainian: …;
# Russian: …)").
NATIVE_RE = re.compile(r"^\((.+)\)$")
NATIVE_LABEL_RE = re.compile(r"^(?:Russian|Ukrainian|French): (.+)$")
# The language-labelled parenthetical can also trail the name on its first
# line. Only language-labelled tails are peeled; other parentheticals
# ("(Pravfond)", "(RTRS)") are name text and stay whole.
NAME_NATIVE_TAIL_RE = re.compile(r"^(.+) \((?:Russian|Ukrainian|French): (.+)\)$")

# A.45's parenthetical wraps across two printed lines ("(Russian: …" /
# "Ukrainian: …)"); each fragment is one rendering.
NATIVE_SPLIT_PINS = frozenset({("A", "45")})
NATIVE_OPEN_RE = re.compile(r"^\((?:Russian|Ukrainian|French): (.+)$")
NATIVE_CLOSE_RE = re.compile(r"^(?:Russian|Ukrainian|French): (.+)\)$")
# B.4 prints its alias label as a bare "aka" inside the line ("AFA Media aka
# RED"); elsewhere " aka " could be name text, so the split is pinned.
INLINE_BARE_AKA_PINS = frozenset({("B", "4")})
BARE_AKA_RE = re.compile(r"^(.+?) aka (.+)$")

# Identifying-information labels → CSV column, exactly as printed.
INFO_LABELS = {
    "Function": "position",
    "Position": "position",
    "Rank": "position",
    "DOB": "birthDate",
    "Date of birth": "birthDate",
    "POB": "birthPlace",
    "Place of birth": "birthPlace",
    "Nationality": "nationality",
    "Gender": "gender",
    # A.64 prints the label with a typo; the value is still the gender.
    "Geder": "gender",
    "Passport number": "passportNumber",
    "Passport No.": "passportNumber",
    "ID-No.": "idNumber",
    "Identity documents": "idNumber",
    "Residence Permit": "idNumber",
    "Tax Identification Number (INN)": "innCode",
    "Russian Tax ID (ИНН)": "innCode",
    "INN": "innCode",
    "Tax identification number": "taxNumber",
    "Individual Taxpayer Number": "taxNumber",
    "Ukrainian Tax ID (Код ДРФО)": "taxNumber",
    "VAT Nr.": "taxNumber",
    "Address": "address",
    # Mostly cities and street addresses, only rarely a bare country — a
    # place, not a jurisdiction.
    "Place of registration": "address",
    "Place of Registration": "address",
    "Principal place of business": "address",
    "Principle place of business": "address",
    "Date of registration": "incorporationDate",
    "Date of creation": "incorporationDate",
    "Type of entity": "legalForm",
    "Registration number": "registrationNumber",
    "Registration No": "registrationNumber",
    # B.8's label reads "BIN" but no more specific identifier system is
    # established by the source.
    "BIN": "registrationNumber",
    "OGRN": "ogrnCode",
    "KPP": "kppCode",
    "Website": "website",
    "Phone number": "phone",
}
# Columns whose labelled value legitimately continues onto bare follow-on
# lines in this document (native-script address renderings, a former
# address, a "(maildrop address)" annotation). Bare lines after any other
# label are new structure.
CONTINUABLE_COLUMNS = frozenset({"address"})
# Relational labels naming other parties: no CSV column, deliberately not
# transcribed ("Founder: Hüseyin Dogru" names the A.20 designee).
DROP_LABELS = frozenset({"Founder"})
# A.10/A.11 print passports as colon-less lines with the issuing country in
# a parenthetical ("Passport No 753870064 (Russian Federation)").
PASSPORT_LINE_RE = re.compile(r"^Passport No ([A-Z0-9]+ \(.+\))$")
# Reviewed hand-mappings for identifying-information lines the label rules
# cannot place, keyed by (part, entry) and the exact line. If the source
# line changes, the lookup misses and the run breaks for re-review.
INFO_OVERRIDES: dict[tuple[str, str], dict[str, tuple[tuple[str, str], ...]]] = {
    # A colon-less registration number after the VAT line.
    ("B", "4"): {"Registration number 423277-5": (("registrationNumber", "423277-5"),)},
    # A paired-label line holding two identifiers.
    ("B", "6"): {
        "TIN / KPP: 2901170107 / 519001001": (
            ("innCode", "2901170107"),
            ("kppCode", "519001001"),
        ),
    },
    # A "Registration number" line that itself carries labelled identifiers.
    ("B", "10"): {
        "Registration number: ИНН: 7717127211; ОГРН: 1027739456084": (
            ("innCode", "7717127211"),
            ("ogrnCode", "1027739456084"),
        ),
    },
    # A descriptive trailer after the phone line; no structured column.
    ("B", "11"): {"Military unit 09643": (("notes", "Military unit 09643"),)},
    # The whole cell is one bare address line without a label.
    ("B", "16"): {
        "236000, Posyolok Kumachevo, Kaliningrad region, Russian Federation": (
            (
                "address",
                "236000, Posyolok Kumachevo, Kaliningrad region, Russian Federation",
            ),
        ),
    },
}


def verbatim_date(text: str, ctx: str) -> str:
    # Only the dotted form occurs in this document. The printed wording is
    # kept; the recognizer only guards the shape.
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
    schemata = [schema_name for _, _, _, schema_name in PARTS] + ["Company"]
    for schema_name in schemata:
        if model.get(schema_name) is None:
            raise ParseError(f"unknown schema {schema_name!r}")


def strip_list_suffix(text: str) -> str:
    # Trailing ";" or "," is list punctuation continuing onto the next
    # printed line, not name text.
    return text.rstrip(";,").strip()


def split_akas(text: str) -> list[str]:
    # Alias lists split on ";" only; a comma-joined piece stays whole as one
    # value and is categorised in the crawler's review system.
    return [piece.strip() for piece in text.split(";") if piece.strip()]


def native_renderings(inner: str) -> list[str]:
    # Renderings inside one parenthetical split on ";", each dropping its
    # printed language label.
    out: list[str] = []
    for piece in split_akas(inner):
        label = NATIVE_LABEL_RE.match(piece)
        out.append(label.group(1) if label is not None else piece)
    return out


def peel_native_tail(name: str, row: Row) -> str:
    """Move a trailing "(Russian: …)" language parenthetical off the name."""
    tail = NAME_NATIVE_TAIL_RE.match(name)
    if tail is None:
        return name
    row.add("alias", native_renderings(tail.group(2)))
    return tail.group(1)


def parse_name(ctx: str, part: str, record_id: str, td: Element, row: Row) -> None:
    lines = cell_lines(td, ctx)
    if not lines:
        raise ParseError(f"{ctx}: empty name cell")
    aliases: list[str] = []

    def add_variant(text: str) -> None:
        # An inline bare "aka" splits the pinned line into name + alias text.
        if (part, record_id) in INLINE_BARE_AKA_PINS:
            bare = BARE_AKA_RE.match(text)
            if bare is not None:
                aliases.append(strip_list_suffix(bare.group(1)))
                aliases.extend(split_akas(bare.group(2)))
                return
        aliases.append(strip_list_suffix(text))

    name = peel_native_tail(strip_list_suffix(lines[0]), row)
    if (part, record_id) in INLINE_BARE_AKA_PINS:
        bare = BARE_AKA_RE.match(name)
        if bare is not None:
            name = strip_list_suffix(bare.group(1))
            aliases.extend(split_akas(bare.group(2)))
    for line in lines[1:]:
        if ALIAS_HEAD_RE.match(line) is not None:
            # A bare alias heading; the aliases follow as their own lines.
            continue
        aka = ALIAS_LINE_RE.match(line)
        if aka is not None:
            aliases.extend(split_akas(aka.group(1)))
            continue
        if (part, record_id) in NATIVE_SPLIT_PINS:
            fragment = NATIVE_OPEN_RE.match(line) or NATIVE_CLOSE_RE.match(line)
            if fragment is not None:
                aliases.append(fragment.group(1))
                continue
        native = NATIVE_RE.match(line)
        if native is not None:
            aliases.extend(native_renderings(native.group(1)))
            continue
        opens, closes = line.startswith("("), line.endswith(")")
        if (opens and ")" not in line) or (closes and "(" not in line):
            raise ParseError(f"{ctx}: unbalanced name parenthetical {line[:60]!r}")
        # An unlabeled bare line is a further printed rendering of the name —
        # one variant per line in this document's name column.
        add_variant(line)
    row.add("name", [name])
    row.add("alias", aliases)


def parse_info(ctx: str, part: str, record_id: str, td: Element, row: Row) -> None:
    lines = cell_lines(td, ctx)
    overrides = INFO_OVERRIDES.get((part, record_id), {})
    # Part A entries from the first listing rounds print the person's
    # function as unlabeled lines above the first labelled line; a line
    # ending in "," wraps mid-phrase onto the next line.
    leading = part == "A"
    # A labelled line opens a block; bare lines continue the block for the
    # columns the document has shown continuations for.
    block: str | None = None
    wrapped: str | None = None
    for line in lines:
        if line in overrides:
            for column, value in overrides[line]:
                row.add(column, [value])
            block, wrapped, leading = None, None, False
            continue
        labelled = LABELLED_RE.match(line)
        label = labelled.group(1) if labelled is not None else None
        if label in DROP_LABELS:
            block, wrapped, leading = None, None, False
            continue
        if label is not None and label in INFO_LABELS:
            assert labelled is not None
            column = INFO_LABELS[label]
            value = labelled.group(2)
            if value == "":
                raise ParseError(f"{ctx}: empty value for label {label!r}")
            row.add(column, split_values(value))
            block, wrapped, leading = column, None, False
            continue
        passport = PASSPORT_LINE_RE.match(line)
        if passport is not None:
            row.add("passportNumber", [passport.group(1)])
            block, wrapped, leading = None, None, False
            continue
        if wrapped is not None:
            row.props[wrapped][-1] = f"{row.props[wrapped][-1]} {line}"
            wrapped = None
            continue
        if leading:
            # A line ending in "," wraps mid-phrase; the comma is kept and
            # the next line joins onto it.
            row.add("position", [line if line.endswith(",") else line.rstrip(";")])
            wrapped = "position" if line.endswith(",") else None
            continue
        if block is not None and block in CONTINUABLE_COLUMNS:
            row.add(block, split_values(line))
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
    if (part, record_id) in COMPANY_PINS:
        schema = "Company"
    row = Row(annex_id(roman, part), schema, MEASURE, record_id=record_id)
    parse_name(ctx, part, record_id, cells[1], row)
    parse_info(ctx, part, record_id, cells[2], row)
    row.reason = " ".join(cell_lines(cells[3], ctx))
    row.start_date = verbatim_date(cell_line(cells[4], ctx), ctx)
    return row


def parse_part(roman: str, grid: Element, spec: tuple[str, str, str, str]) -> list[Row]:
    letter, heading, part, schema = spec
    ctx = f"{roman}.{part}"
    col1 = xpath_elements(grid, "./div[contains(@class, 'grid-list-column-1')]")
    if len(col1) != 1 or clean(element_text(col1[0]), ctx) != letter:
        raise ParseError(f"{ctx}: expected part letter {letter!r}")
    col2 = xpath_elements(grid, "./div[contains(@class, 'grid-list-column-2')]")
    if len(col2) != 1:
        raise ParseError(f"{ctx}: expected one part content column")
    rows: list[Row] = []
    seen_heading = False
    for child in col2[0].iterchildren():
        if not isinstance(child.tag, str):
            continue
        cls = child.get("class") or ""
        if child.tag == "p" and cls == "modref":
            check_marker(" ".join(element_text(child).split()), ctx)
            continue
        if child.tag == "p" and cls == "norm":
            if clean(element_text(child), ctx) != heading or seen_heading:
                raise ParseError(f"{ctx}: unexpected part heading")
            seen_heading = True
            continue
        if child.tag == "p" and cls in SKIP_P_CLASSES:
            continue
        if child.tag == "div" and cls == "centered":
            table = xpath_elements(child, ".//table", expect_exactly=1)[0]
            for tr in table_body(ctx, table, PART_HEADER):
                rows.append(parse_row(roman, part, schema, tr))
            continue
        raise ParseError(f"{ctx}: unexpected <{child.tag} class={cls!r}>")
    if not seen_heading:
        raise ParseError(f"{ctx}: missing part heading {heading!r}")
    return rows


def parse_annex_i(roman: str, block: Element) -> list[Row]:
    rows: list[Row] = []
    grids: list[Element] = []
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
        if child.tag == "div" and "grid-container" in cls:
            grids.append(child)
            continue
        raise ParseError(f"{roman}: unexpected <{child.tag} class={cls!r}>")
    if len(grids) != len(PARTS):
        raise ParseError(f"{roman}: {len(grids)} part blocks, expected {len(PARTS)}")
    for spec, grid in zip(PARTS, grids, strict=True):
        rows.extend(parse_part(roman, grid, spec))
    return rows


def check_placeholder_annex(roman: str, block: Element) -> None:
    """Accept a reserved list that prints only the "[…]" placeholder."""
    placeholders = 0
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
            if clean(element_text(child), roman) != PLACEHOLDER:
                raise ParseError(f"{roman}: reserved annex now has content")
            placeholders += 1
            continue
        raise ParseError(f"{roman}: unexpected <{child.tag} class={cls!r}>")
    if placeholders != 1:
        raise ParseError(f"{roman}: expected one placeholder, got {placeholders}")


def parse_document(doc: Element) -> list[Row]:
    rows: list[Row] = []
    known = {"I"} | set(PLACEHOLDER_ANNEXES) | NON_TARGET
    for roman, block in annex_blocks(doc, known):
        if roman in NON_TARGET:
            continue
        if roman in PLACEHOLDER_ANNEXES:
            check_placeholder_annex(roman, block)
            continue
        annex_rows = parse_annex_i(roman, block)
        if not annex_rows:
            raise ParseError(f"{roman}: no entries extracted")
        rows.extend(annex_rows)
    return rows


@click.command(help="Parse consolidated Regulation 2024/2642 into a CSV candidate.")
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
            raise ParseError(f"not a consolidated 2024/2642 CELEX: {celex!r}")
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
