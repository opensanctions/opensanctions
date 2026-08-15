"""Parse consolidated Regulation (EU) 2017/1509 (North Korea) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation is a hybrid: designation annexes among many sectoral goods
and financial annexes. The designation annexes:

- Annex XIII — the UN fund-freeze list (Articles 34(1)/(3)), parts
  (a) Natural persons and (b) Legal persons, entities and bodies, each a
  six-column table. Part (b)'s narrative column is headed "Other
  information" but holds the designation rationale, parallel to part (a)'s
  "Statement of reasons", and maps to `reason`. Entry 20 (Ocean Maritime
  Management) is followed by a "Vessels with IMO Number:" sub-list of
  UN-designated vessels printed as lettered continuation rows with their
  own designation dates; each becomes a Vessel row (no printed entry
  number, so recordId stays empty).
- Annex XIV — vessels (Article 34(2) and Article 39(1)(g)), two grid-list
  parts: A. Vessels subject to a seizure (fund freeze) and B. Vessels
  prohibited entry into ports (a port ban → "Transportation restrictions").
  Part A's "Designated as economic resources of" column names the owning
  entity — relational, deliberately not transcribed. Part B prints an MMSI
  parenthetical under the IMO number → mmsi. Unlabelled and
  "Other information:" prose under the vessel names is descriptive → notes.
- Annex XV — the EU-autonomous list (Article 34(4)), parts (a)–(f). Part
  (e) prints no table (never used); a table appearing there breaks for
  review. Part (f)'s entries print "Type of ship:" and "IMO:" lines — they
  are vessels despite the part heading's wording, and are emitted with the
  Vessel schema. "Type of ship" has no CSV column and is deliberately not
  transcribed; "Owner:" names another party and is dropped as relational.
- Annex XVI — a further EU-autonomous list (Article 34(1)/(3)), parts
  (a)/(b), five-column tables whose name cell carries the aliases.
- Annexes XVII and XVIII — designation annexes that have never held an
  entry; each prints only its subtitle, pinned exactly.

All other annexes (I–XII including the XIa–XIl series) list goods,
technology, financial-service categories, or authority websites — no
designations. Dates are transcribed as the source prints them (dotted form
only); XIII part (a) entry 53 prints no designation date. In XV/XVI the
name cell's follow-on lines are aliases per the "(and possible aliases)"
header: native-script renderings, "a.k.a." lines and "(alias …)"
parentheticals; printed alias labels inside the name line move their
content to alias. Entry XIII.A 2's Korean and Chinese renderings print as
images — untranscribable, so only their empty script labels remain and are
deliberately not transcribed. Identity-document validity lines and
kin/relational content are deliberately not transcribed, per the contract.

Output: data/consolidated/32017R1509.csv (the EU Journal consolidated CSV
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
    cell_line,
    cell_lines,
    check_consolidated_celex,
    check_marker,
    check_registry,
    clean,
    load_source,
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

FRAMEWORK_CELEX = "32017R1509"
PROGRAM_KEY = "EU-PRK"

# Articles 34(1)-(4) are the fund freeze; Article 39(1)(g) prohibits port
# entry for the Annex XIV part B vessels. Travel bans live in Decision
# (CFSP) 2016/849.
MEASURE_FREEZE = "Asset freeze"
MEASURE_PORT = "Transportation restrictions"

# Goods, technology, financial-service and authority-website annexes.
NON_TARGET = frozenset(
    {
        "I",
        "II",
        "III",
        "IV",
        "V",
        "VI",
        "VII",
        "VIII",
        "IX",
        "X",
        "XI",
        "XIa",
        "XIb",
        "XIc",
        "XId",
        "XIe",
        "XIf",
        "XIg",
        "XIh",
        "XIi",
        "XIj",
        "XIk",
        "XIl",
        "XII",
    }
)
TARGETS = frozenset({"XIII", "XIV", "XV", "XVI"})
# Designation annexes that have never held an entry: annex → the exact
# printed subtitle, the only content after the title.
EMPTY_ANNEXES = {
    "XVII": "List of persons, entities or bodies referred to in Article 34(1) and 34(3)",
    "XVIII": "Vessels referred to in points (d), (e) and (f) of Article 43(1)",
}

XIII_SUBTITLE = (
    "List of persons, entities and bodies referred to in Article 34(1) and 34(3)"
)
XIII_PARTS = (
    ("(a) Natural persons", "A", "Person"),
    ("(b) Legal persons, entities and bodies", "B", "LegalEntity"),
)
XIII_HEADER_A = (
    "",
    "Name",
    "Alias",
    "Identifying information",
    "Date of UN designation",
    "Statement of reasons",
)
XIII_HEADER_B = (
    "",
    "Name",
    "Alias",
    "Location",
    "Date of UN designation",
    "Other information",
)

XIV_INTRO = (
    "The vessels referred to in Article 34(2) and point (g) of Article 39(1)"
    " and applicable measures as specified by the Sanctions Committee"
)
XIV_PARTS = (
    (
        "A.",
        "Vessels subject to a seizure",
        "A",
        MEASURE_FREEZE,
        (
            "",
            "Vessel name",
            "IMO number",
            "Designated as economic resources of",
            "Date of UN designation",
        ),
    ),
    (
        "B.",
        "Vessels which are prohibited entry into ports",
        "B",
        MEASURE_PORT,
        ("", "Vessel name", "IMO number", "Date of UN designation"),
    ),
)

XV_SUBTITLE = (
    "List of persons, entities and bodies referred to in Article 34(1) and 34(3)"
)
# (heading, part id, schema, has table). Part (e) has never held an entry;
# part (f)'s entries print "Type of ship:" lines — vessels, not legal
# persons, despite the heading's wording.
XV_PARTS = (
    (
        "(a) Natural persons designated in accordance with point (a) of Article 34(4)",
        "A",
        "Person",
        True,
    ),
    (
        "(b) Legal persons, entities and bodies designated in accordance with point (a) of Article 34(4)",
        "B",
        "LegalEntity",
        True,
    ),
    (
        "(c) Natural persons designated in accordance with point (b) of Article 34(4)",
        "C",
        "Person",
        True,
    ),
    (
        "(d) Legal persons, entities and bodies designated in accordance with point (b) of Article 34(4)",
        "D",
        "LegalEntity",
        True,
    ),
    (
        "(e) Natural persons designated in accordance with point (c) of Article 34(4)",
        "E",
        "Person",
        False,
    ),
    (
        "(f) Legal persons, entities and bodies designated in accordance with point (c) of Article 34(4)",
        "F",
        "Vessel",
        True,
    ),
)
XV_HEADER_PERSON = (
    "",
    "Name (and possible aliases)",
    "Alias",
    "Identifying information",
    "Date of designation",
    "Reasons",
)
XV_HEADER_ENTITY = (
    "",
    "Name (and possible aliases)",
    "Alias",
    "Location",
    "Date of designation",
    "Reasons",
)
XV_HEADER_VESSEL = (
    "",
    "Name",
    "Alias",
    "Identifying information",
    "Date of designation",
    "Reasons",
)

XVI_SUBTITLE = (
    "List of persons, entities or bodies referred to in Article 34(1) and 34(3)"
)
XVI_PARTS = (
    ("(a) Natural persons", "A", "Person"),
    ("(b) Legal persons, entities and bodies.", "B", "LegalEntity"),
)
XVI_HEADER_A = (
    "",
    "Name (and possible aliases)",
    "Identifying information",
    "Date of designation",
    "Reasons",
)
XVI_HEADER_B = (
    "",
    "Name (and possible aliases)",
    "Location",
    "Date of designation",
    "Reasons",
)

# Labels in person identifying-information cells → CSV column.
PERSON_INFO_LABELS = {
    "DOB": "birthDate",
    "Date of birth": "birthDate",
    "Date of Birth": "birthDate",
    "YOB": "birthDate",
    "POB": "birthPlace",
    "Place of birth": "birthPlace",
    "Nationality": "nationality",
    "Gender": "gender",
    "Address": "address",
    "Location": "address",
    "Passport": "passportNumber",
    "Passport no": "passportNumber",
    "Passport No": "passportNumber",
    "Passport No.": "passportNumber",
    "Passport number": "passportNumber",
    "Passport numbers": "passportNumber",
    "Service passport number": "passportNumber",
    "Diplomatic Passport number": "passportNumber",
    "Diplomatic passport number": "passportNumber",
    "Function or profession": "position",
    "Telephone": "phone",
    "Email": "email",
}
# Labels in entity location cells → CSV column; bare lines are addresses.
# "IMO number" on an entity is its IMO *company* number — a registry
# identifier; imoNumber is not a LegalEntity property in FtM.
LOCATION_LABELS = {
    "Address": "address",
    "Alternate address": "address",
    "Alternate Address": "address",
    "Location": "address",
    "IMO number": "registrationNumber",
    "SWIFT": "swiftBic",
    "SWIFT/BIC": "swiftBic",
    "Telephone": "phone",
    "Telephone number": "phone",
    "Telephone numbers": "phone",
    "Facsimile number": "phone",
    "Fax": "phone",
    "Email": "email",
    "Email addresses": "email",
    "Website": "website",
}
# Labels in Annex XV part (f) vessel identifying-information cells.
VESSEL_INFO_LABELS = {
    "IMO": "imoNumber",
    "Principal place of business": "address",
}
# Labels with no CSV column, deliberately not transcribed: identity-document
# validity attributes, the vessel type (no column; the printed value is a
# ship-class phrase), and "Owner", which names another party (relational).
DROP_LABELS = frozenset(
    {
        "Passport date of expiration",
        "Passport date of issue",
        "Date of expiration",
        "Type of ship",
        "Owner",
    }
)

# Colon-less label families printed in XIII part (a) info cells.
BARE_DOB_RE = re.compile(r"^DOB\.? (.+)$")
BARE_PASSPORT_RE = re.compile(r"^Passport(?: [Nn]o\.?| number)? (.+)$")
BARE_FAX_RE = re.compile(r"^Fax (.+)$")
# Lettered continuation values under an empty-valued label ("Passport no:").
LETTERED_VALUE_RE = re.compile(r"^([a-z])\) (.+)$")

# Reviewed hand-mappings for bare prose lines in identifying-information
# cells, keyed by (annex id, entry number) and the exact line; the empty
# string drops the line deliberately (identity-document validity). If the
# source line changes, the lookup misses and the run breaks for re-review.
BARE_PROSE_PINS: dict[tuple[str, str], dict[str, str]] = {
    ("XIII.A", "52"): {
        "Served as Korea Ryonbong General Corporation representative in Cuba": "position",
    },
    ("XV.A", "35"): {
        "Deputy Consul at DPRK Consulate General in Nakhodka, Russian Federation": "position",
    },
    ("XV.A", "39"): {
        (
            "Deputy Representative for the Korea Mining Development Trading"
            " Corporation (KOMID) in Syria"
        ): "position",
    },
    ("XV.C", "5"): {
        "Diplomat, DPRK Embassy, Belarus": "position",
    },
    ("XV.C", "7"): {
        (
            "Foreign Trade Bank of the Democratic People’s Republic of Korea"
            " Representative in Khabarovsk, Russian Federation"
        ): "position",
    },
    ("XVI.A", "4"): {
        "Diplomat DPRK Embassy, Angola": "position",
    },
    ("XVI.A", "21"): {
        # Passport validity attribute; the contract drops document validity.
        "Valid until 12.2.2020": "",
    },
    ("XVI.A", "25"): {
        "Co-founder of the CONGO ACONDE company": "position",
    },
    ("XVI.A", "26"): {
        "Co-founder of the CONGO ACONDE company": "position",
    },
    ("XVI.A", "32"): {
        (
            "Intermediary for the construction project of an ammunition factory in Mali"
        ): "position",
    },
    ("XVI.A", "33"): {
        "Governor, Primorsky Krai, Russian Federation": "position",
    },
}

# XIII part (a) entry 53 (Ri Yong Mu) prints no designation date.
EMPTY_DATE_PINS = frozenset({("XIII.A", "53")})
# Name-cell alias lines the native-script rule cannot classify: renderings
# whose legal-form prefix prints in Latin letters ("OOO ‘…’").
ALIAS_LINE_PINS = frozenset({("XVI.B", "10")})

NUMBER_RE = re.compile(r"^(\d+)\.$")
# Printed absence placeholders, dropped per the contract.
NOT_AVAILABLE = frozenset({"na", "n/a"})
# The OMM vessel sub-list rows: "(a) Chol Ryong (Ryong Gun Bong) 8606173" —
# lettered marker, vessel name (parentheticals stay in the name), and the
# IMO number promised by the printed "Vessels with IMO Number:" sub-heading.
VESSEL_SUBROW_RE = re.compile(r"^\(([a-z]{1,2})\) (.+) (\d{7})$")
VESSEL_SUBLIST_ENTRY = ("XIII", "B", "20")
VESSEL_SUBLIST_HEADING = "Vessels with IMO Number:"
# Part B of Annex XIV prints the vessel's MMSI as a parenthetical line under
# the IMO number; the digits go to the mmsi column, the parenthetical and
# label being the line's structure.
MMSI_LINE_RE = re.compile(r"^\(MMSI: (\d+)\)$")
# Name-cell alias lines in XV/XVI ("Name (and possible aliases)" columns).
AKA_LINE_RE = re.compile(r"^a\.k\.a\.?:? (.+)$")
PAREN_ALIAS_RE = re.compile(r"^\((?:alias|Alias):? (.+)\)$")
LATIN_RE = re.compile(r"[A-Za-z]")
# Printed alias labels inside a name line: a trailing "(alias …)" /
# "(a.k.a. …)" parenthetical, or an inline " a.k.a. " tail.
NAME_PAREN_ALIAS_TAIL_RE = re.compile(r"^(.+) \((?:alias|a\.k\.a\.?):? (.+)\)$")
NAME_INLINE_AKA_RE = re.compile(r"^(.+?) a\.k\.a\.?:? (.+)$")
# Lettered alias enumerations inside Alias cells: "a) X; b) Y c) Z".
ALIAS_LETTER_SPLIT_RE = re.compile(r"(?:^|;? )([a-z])\) ")
# Entry XIII.A 2's Korean and Chinese renderings print as images —
# untranscribable — leaving these empty-valued script labels in the Alias
# cell; deliberately not transcribed.
IMAGE_SCRIPT_LABEL_RE = re.compile(r"^(?:Korean|Chinese) name: ?;?$")


# Only the dotted form occurs in this document.
DATE_FORMATS = ("dotted",)


def parse_date_cell(ctx: str, annex_part: str, record_id: str, td: Element) -> str:
    lines = cell_lines(td, ctx)
    if not lines:
        if (annex_part, record_id) in EMPTY_DATE_PINS:
            return ""
        raise ParseError(f"{ctx}: empty designation date")
    if len(lines) != 1:
        raise ParseError(f"{ctx}: {len(lines)} lines in date cell")
    return verbatim_date(lines[0], ctx, DATE_FORMATS)


def split_alias_line(ctx: str, line: str) -> list[str]:
    """Split one Alias-cell line on its printed list structure.

    Lettered enumerations ("a) X; b) Y") split at their in-sequence markers;
    otherwise values split on ";" only — comma-joined pieces stay whole.
    """
    if re.match(r"^a\) ", line):
        markers = [
            (m.start(1), m.group(1), m.end())
            for m in ALIAS_LETTER_SPLIT_RE.finditer(line)
        ]
        expected = [chr(ord("a") + i) for i in range(len(markers))]
        if [m[1] for m in markers] != expected:
            raise ParseError(
                f"{ctx}: lettered alias list out of sequence {line[:60]!r}"
            )
        values: list[str] = []
        for index, (_, _, end) in enumerate(markers):
            stop = markers[index + 1][0] if index + 1 < len(markers) else len(line)
            value = line[end:stop].strip().rstrip(";").rstrip(",").strip()
            if value:
                values.append(value)
        return values
    return [piece.strip().rstrip(",") for piece in line.split(";") if piece.strip()]


def parse_alias_cell(ctx: str, td: Element, row: Row) -> None:
    for line in cell_lines(td, ctx):
        if IMAGE_SCRIPT_LABEL_RE.match(line) is not None:
            continue
        aka = AKA_LINE_RE.match(line)
        if aka is not None:
            row.add("alias", split_alias_line(ctx, aka.group(1)))
            continue
        row.add("alias", split_alias_line(ctx, line))


def parse_multiline_name(
    ctx: str, annex_part: str, record_id: str, td: Element, row: Row
) -> None:
    """Name cell of the "(and possible aliases)" columns in XV and XVI.

    The first line is the name; every further line is an alias, printed as
    a native-script rendering, an "a.k.a." line, or an "(alias …)"
    parenthetical, per the column header's promise.
    """
    lines = cell_lines(td, ctx)
    if not lines:
        raise ParseError(f"{ctx}: empty name cell")
    name = lines[0]
    # A printed alias label inside the name line moves its content to alias.
    tail = NAME_PAREN_ALIAS_TAIL_RE.match(name)
    if tail is not None:
        name = tail.group(1)
        row.add("alias", split_alias_line(ctx, tail.group(2)))
    inline = NAME_INLINE_AKA_RE.match(name)
    if inline is not None:
        name = inline.group(1)
        row.add("alias", split_alias_line(ctx, inline.group(2)))
    row.add("name", [name])
    for line in lines[1:]:
        aka = AKA_LINE_RE.match(line)
        if aka is not None:
            row.add("alias", split_alias_line(ctx, aka.group(1)))
            continue
        paren = PAREN_ALIAS_RE.match(line)
        if paren is not None:
            row.add("alias", split_alias_line(ctx, paren.group(1)))
            continue
        if LATIN_RE.search(line) is None or (annex_part, record_id) in ALIAS_LINE_PINS:
            row.add("alias", [line])
            continue
        raise ParseError(f"{ctx}: unrecognized name line {line[:60]!r}")


def parse_person_info(
    ctx: str, annex_part: str, record_id: str, td: Element, row: Row
) -> None:
    pins = BARE_PROSE_PINS.get((annex_part, record_id), {})
    block_column: str | None = None
    block_letters: list[str] = []
    for line in cell_lines(td, ctx):
        labelled = LABELLED_RE.match(line)
        label = labelled.group(1) if labelled is not None else None
        if label in DROP_LABELS:
            block_column = None
            continue
        if label is not None and label in PERSON_INFO_LABELS:
            assert labelled is not None
            column = PERSON_INFO_LABELS[label]
            value = labelled.group(2)
            if value in NOT_AVAILABLE:
                block_column = None
                continue
            if value:
                row.add(column, [value.rstrip(";").rstrip(",")])
                # A trailing list comma continues the value list on the
                # next printed line.
                block_column = column if value.endswith(",") else None
                block_letters = []
            else:
                # An empty-valued label holds its values on following lines.
                block_column = column
                block_letters = []
            continue
        if label is not None and label not in pins and line not in pins:
            raise ParseError(f"{ctx}: unrecognized info label {label!r}")
        if line in pins:
            column = pins[line]
            if column:
                row.add(column, [line])
            block_column = None
            continue
        lettered = LETTERED_VALUE_RE.match(line)
        if block_column is not None and lettered is not None:
            expected = chr(ord("a") + len(block_letters))
            if lettered.group(1) != expected:
                raise ParseError(f"{ctx}: lettered value out of sequence {line[:50]!r}")
            block_letters.append(lettered.group(1))
            row.add(block_column, [lettered.group(2).rstrip(";")])
            continue
        if block_column is not None:
            row.add(block_column, [line.rstrip(";")])
            block_column = None
            continue
        dob = BARE_DOB_RE.match(line)
        if dob is not None:
            row.add("birthDate", [dob.group(1)])
            continue
        passport = BARE_PASSPORT_RE.match(line)
        if passport is not None:
            row.add("passportNumber", [passport.group(1).rstrip(";")])
            continue
        fax = BARE_FAX_RE.match(line)
        if fax is not None:
            row.add("phone", [fax.group(1)])
            continue
        raise ParseError(f"{ctx}: unrecognized info line {line[:60]!r}")


def parse_location_cell(ctx: str, td: Element, row: Row) -> None:
    """Entity Location cells: labelled contact lines, bare lines are
    addresses. An empty-valued label holds its value on the next line."""
    block_column: str | None = None
    for line in cell_lines(td, ctx):
        labelled = LABELLED_RE.match(line)
        label = labelled.group(1) if labelled is not None else None
        if label is not None and label in LOCATION_LABELS:
            if block_column is not None:
                raise ParseError(f"{ctx}: label {label!r} inside open block")
            assert labelled is not None
            value = labelled.group(2)
            if value == "":
                block_column = LOCATION_LABELS[label]
            else:
                row.add(LOCATION_LABELS[label], [value.rstrip(";")])
            continue
        if block_column is not None:
            row.add(block_column, [line.rstrip(";")])
            block_column = None
            continue
        fax = BARE_FAX_RE.match(line)
        if fax is not None:
            row.add("phone", [fax.group(1)])
            continue
        row.add("address", [line.rstrip(";")])
    if block_column is not None:
        raise ParseError(f"{ctx}: empty-valued label closes the cell")


def parse_vessel_info(ctx: str, td: Element, row: Row) -> None:
    for line in cell_lines(td, ctx):
        labelled = LABELLED_RE.match(line)
        label = labelled.group(1) if labelled is not None else None
        if label in DROP_LABELS:
            continue
        if label is not None and label in VESSEL_INFO_LABELS:
            assert labelled is not None
            value = labelled.group(2)
            if value == "":
                raise ParseError(f"{ctx}: empty value for label {label!r}")
            row.add(VESSEL_INFO_LABELS[label], [value])
            continue
        raise ParseError(f"{ctx}: unrecognized vessel info line {line[:60]!r}")


def parse_reason_cell(ctx: str, td: Element) -> str:
    return " ".join(cell_lines(td, ctx))


def part_tables(
    roman: str, block: Element, subtitle: str, headings: list[str]
) -> dict[str, Element]:
    """Walk a table-annex block, returning the table under each part heading."""
    tables: dict[str, Element] = {}
    current: str | None = None
    for child in block.iterchildren():
        if not isinstance(child.tag, str):
            continue
        cls = child.get("class") or ""
        if child.tag == "hr" and cls == "separator-annex":
            continue
        if child.tag == "p" and cls == "modref":
            check_marker(" ".join(element_text(child).split()), roman)
            continue
        if child.tag == "p" and cls == "title-gr-seq-level-1":
            if clean(element_text(child), roman) != subtitle:
                raise ParseError(f"{roman}: unexpected subtitle")
            continue
        if child.tag == "p" and cls == "title-gr-seq-level-2":
            heading = clean(element_text(child), roman)
            if heading not in headings:
                raise ParseError(f"{roman}: unknown part heading {heading[:60]!r}")
            current = heading
            continue
        if child.tag == "p" and cls in SKIP_P_CLASSES:
            continue
        if child.tag == "div" and cls == "centered":
            if current is None:
                raise ParseError(f"{roman}: table before any part heading")
            if current in tables:
                raise ParseError(f"{roman}: second table under {current[:40]!r}")
            found = xpath_elements(child, ".//table", expect_exactly=1)
            tables[current] = found[0]
            continue
        raise ParseError(f"{roman}: unexpected <{child.tag} class={cls!r}>")
    return tables


def entry_number(ctx: str, td: Element, last: int) -> str:
    line = cell_line(td, ctx)
    match = NUMBER_RE.match(line)
    if match is None:
        raise ParseError(f"{ctx}: unrecognized entry number {line!r}")
    number = int(match.group(1))
    if number <= last:
        raise ParseError(f"{ctx}: entry number {number} not increasing")
    return match.group(1)


def parse_annex_xiii(roman: str, block: Element) -> list[Row]:
    rows: list[Row] = []
    tables = part_tables(roman, block, XIII_SUBTITLE, [h for h, _, _ in XIII_PARTS])
    for heading, part, schema in XIII_PARTS:
        if heading not in tables:
            raise ParseError(f"{roman}: missing part {heading[:40]!r}")
        annex_part = f"{roman}.{part}"
        header = XIII_HEADER_A if part == "A" else XIII_HEADER_B
        last = 0
        in_vessel_sublist = False
        for tr in table_body(annex_part, tables[heading], header):
            cells = xpath_elements(tr, "./td|./th")
            first = clean(element_text(cells[0]), annex_part)
            if first == "":
                subrow = parse_vessel_subrow(
                    annex_part, str(last), in_vessel_sublist, cells
                )
                if subrow is None:
                    in_vessel_sublist = True
                else:
                    rows.append(subrow)
                continue
            in_vessel_sublist = False
            record_id = entry_number(annex_part, cells[0], last)
            last = int(record_id)
            ctx = f"{annex_part} entry {record_id}"
            row = Row(annex_part, schema, MEASURE_FREEZE, record_id=record_id)
            row.add("name", [cell_line(cells[1], ctx)])
            parse_alias_cell(ctx, cells[2], row)
            if part == "A":
                parse_person_info(ctx, annex_part, record_id, cells[3], row)
            else:
                parse_location_cell(ctx, cells[3], row)
            row.start_date = parse_date_cell(ctx, annex_part, record_id, cells[4])
            row.reason = parse_reason_cell(ctx, cells[5])
            rows.append(row)
    return rows


def parse_vessel_subrow(
    annex_part: str, after: str, in_sublist: bool, cells: list[Element]
) -> Row | None:
    """One row of the OMM "Vessels with IMO Number:" sub-list.

    Returns None for the sub-heading row that opens the list.
    """
    ctx = f"{annex_part} vessel sub-list after entry {after}"
    roman, _, part = annex_part.partition(".")
    if (roman, part, after) != VESSEL_SUBLIST_ENTRY:
        raise ParseError(f"{ctx}: unexpected blank-number row")
    texts = [clean(element_text(cell), ctx) for cell in cells]
    if texts[2] or texts[3] or texts[5]:
        raise ParseError(f"{ctx}: unexpected content in sub-list row")
    if texts[1] == VESSEL_SUBLIST_HEADING:
        if in_sublist or texts[4]:
            raise ParseError(f"{ctx}: unexpected sub-list heading")
        return None
    if not in_sublist:
        raise ParseError(f"{ctx}: vessel row before the sub-list heading")
    match = VESSEL_SUBROW_RE.match(texts[1])
    if match is None:
        raise ParseError(f"{ctx}: unrecognized vessel line {texts[1][:60]!r}")
    row = Row(annex_part, "Vessel", MEASURE_FREEZE)
    row.add("name", [match.group(2)])
    row.add("imoNumber", [match.group(3)])
    row.start_date = verbatim_date(texts[4], ctx, DATE_FORMATS)
    return row


def parse_annex_xiv(roman: str, block: Element) -> list[Row]:
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
        if child.tag == "p" and cls == "norm":
            if clean(element_text(child), roman) != XIV_INTRO:
                raise ParseError(f"{roman}: unexpected intro paragraph")
            continue
        if child.tag == "p" and cls in SKIP_P_CLASSES:
            continue
        if child.tag == "div" and "grid-container" in cls:
            grids.append(child)
            continue
        raise ParseError(f"{roman}: unexpected <{child.tag} class={cls!r}>")
    if len(grids) != len(XIV_PARTS):
        raise ParseError(f"{roman}: {len(grids)} grids, expected {len(XIV_PARTS)}")
    for (letter, heading, part, measure, header), grid in zip(
        XIV_PARTS, grids, strict=True
    ):
        annex_part = f"{roman}.{part}"
        columns = xpath_elements(grid, "./div")
        if len(columns) != 2:
            raise ParseError(f"{annex_part}: grid has {len(columns)} columns")
        if clean(element_text(columns[0]), annex_part) != letter:
            raise ParseError(f"{annex_part}: part letter mismatch")
        seen_heading = False
        table: Element | None = None
        for child in columns[1].iterchildren():
            if not isinstance(child.tag, str):
                continue
            cls = child.get("class") or ""
            if child.tag == "p" and cls == "modref":
                check_marker(" ".join(element_text(child).split()), annex_part)
                continue
            if child.tag == "p":
                text = clean(element_text(child), annex_part)
                if text == "":
                    continue
                if text == heading and not seen_heading:
                    seen_heading = True
                    continue
                raise ParseError(f"{annex_part}: unexpected paragraph {text[:50]!r}")
            if child.tag == "div" and cls == "centered":
                if table is not None:
                    raise ParseError(f"{annex_part}: second table")
                table = xpath_elements(child, ".//table", expect_exactly=1)[0]
                continue
            if child.tag == "table":
                if table is not None:
                    raise ParseError(f"{annex_part}: second table")
                table = child
                continue
            raise ParseError(f"{annex_part}: unexpected <{child.tag} class={cls!r}>")
        if not seen_heading or table is None:
            raise ParseError(f"{annex_part}: part heading or table missing")
        last = 0
        for tr in table_body(annex_part, table, header):
            cells = xpath_elements(tr, "./td|./th")
            record_id = entry_number(annex_part, cells[0], last)
            last = int(record_id)
            ctx = f"{annex_part} entry {record_id}"
            row = Row(annex_part, "Vessel", measure, record_id=record_id)
            parse_vessel_name_cell(ctx, cells[1], row)
            parse_imo_cell(ctx, part, cells[2], row)
            # Part A's "Designated as economic resources of" column names
            # the owning entity — relational, deliberately not transcribed.
            date_cell = cells[4] if part == "A" else cells[3]
            row.start_date = parse_date_cell(ctx, annex_part, record_id, date_cell)
            rows.append(row)
    return rows


def parse_vessel_name_cell(ctx: str, td: Element, row: Row) -> None:
    lines = cell_lines(td, ctx)
    if not lines:
        raise ParseError(f"{ctx}: empty vessel name cell")
    row.add("name", [lines[0]])
    for line in lines[1:]:
        labelled = LABELLED_RE.match(line)
        if labelled is not None:
            if labelled.group(1) != "Other information":
                raise ParseError(f"{ctx}: unexpected label {labelled.group(1)!r}")
            value = labelled.group(2)
            if value in ("na", "n/a"):
                continue
            row.add("notes", [value])
            continue
        row.add("notes", [line])


def parse_imo_cell(ctx: str, part: str, td: Element, row: Row) -> None:
    lines = cell_lines(td, ctx)
    if not lines:
        raise ParseError(f"{ctx}: empty IMO cell")
    row.add("imoNumber", [lines[0]])
    for line in lines[1:]:
        mmsi = MMSI_LINE_RE.match(line)
        if part == "B" and mmsi is not None:
            row.add("mmsi", [mmsi.group(1)])
            continue
        raise ParseError(f"{ctx}: unexpected IMO cell line {line[:40]!r}")


def parse_annex_xv(roman: str, block: Element) -> list[Row]:
    rows: list[Row] = []
    tables = part_tables(roman, block, XV_SUBTITLE, [h for h, _, _, _ in XV_PARTS])
    for heading, part, schema, has_table in XV_PARTS:
        annex_part = f"{roman}.{part}"
        if not has_table:
            if heading in tables:
                raise ParseError(f"{annex_part}: table in part taught as empty")
            continue
        if heading not in tables:
            raise ParseError(f"{annex_part}: missing table")
        if schema == "Person":
            header = XV_HEADER_PERSON
        elif schema == "LegalEntity":
            header = XV_HEADER_ENTITY
        else:
            header = XV_HEADER_VESSEL
        last = 0
        for tr in table_body(annex_part, tables[heading], header):
            cells = xpath_elements(tr, "./td|./th")
            record_id = entry_number(annex_part, cells[0], last)
            last = int(record_id)
            ctx = f"{annex_part} entry {record_id}"
            row = Row(annex_part, schema, MEASURE_FREEZE, record_id=record_id)
            if schema == "Vessel":
                row.add("name", [cell_line(cells[1], ctx)])
            else:
                parse_multiline_name(ctx, annex_part, record_id, cells[1], row)
            parse_alias_cell(ctx, cells[2], row)
            if schema == "Person":
                parse_person_info(ctx, annex_part, record_id, cells[3], row)
            elif schema == "LegalEntity":
                parse_location_cell(ctx, cells[3], row)
            else:
                parse_vessel_info(ctx, cells[3], row)
            row.start_date = parse_date_cell(ctx, annex_part, record_id, cells[4])
            row.reason = parse_reason_cell(ctx, cells[5])
            rows.append(row)
    return rows


def parse_annex_xvi(roman: str, block: Element) -> list[Row]:
    rows: list[Row] = []
    tables = part_tables(roman, block, XVI_SUBTITLE, [h for h, _, _ in XVI_PARTS])
    for heading, part, schema in XVI_PARTS:
        annex_part = f"{roman}.{part}"
        if heading not in tables:
            raise ParseError(f"{annex_part}: missing table")
        header = XVI_HEADER_A if part == "A" else XVI_HEADER_B
        last = 0
        for tr in table_body(annex_part, tables[heading], header):
            cells = xpath_elements(tr, "./td|./th")
            record_id = entry_number(annex_part, cells[0], last)
            last = int(record_id)
            ctx = f"{annex_part} entry {record_id}"
            row = Row(annex_part, schema, MEASURE_FREEZE, record_id=record_id)
            parse_multiline_name(ctx, annex_part, record_id, cells[1], row)
            if part == "A":
                parse_person_info(ctx, annex_part, record_id, cells[2], row)
            else:
                parse_location_cell(ctx, cells[2], row)
            row.start_date = parse_date_cell(ctx, annex_part, record_id, cells[3])
            row.reason = parse_reason_cell(ctx, cells[4])
            rows.append(row)
    return rows


def check_empty_annex(roman: str, block: Element) -> None:
    """A designation annex that has never held an entry: title and the
    pinned subtitle only. Any further content breaks for review."""
    subtitle = EMPTY_ANNEXES[roman]
    seen_subtitle = False
    for child in block.iterchildren():
        if not isinstance(child.tag, str):
            continue
        cls = child.get("class") or ""
        if child.tag == "hr" and cls == "separator-annex":
            continue
        if child.tag == "p" and cls in ("title-gr-seq-level-1", "title-annex-2"):
            if clean(element_text(child), roman) != subtitle or seen_subtitle:
                raise ParseError(f"{roman}: annex subtitle changed")
            seen_subtitle = True
            continue
        if child.tag == "p" and cls == "title-annex-1":
            continue
        if child.tag == "p" and cls == "" and clean(element_text(child), roman) == "":
            continue
        raise ParseError(
            f"{roman}: empty annex now has content <{child.tag} class={cls!r}>"
        )
    if not seen_subtitle:
        raise ParseError(f"{roman}: annex subtitle missing")


def parse_document(doc: Element) -> list[Row]:
    rows: list[Row] = []
    known = set(NON_TARGET) | set(TARGETS) | set(EMPTY_ANNEXES)
    for roman, block in annex_blocks(doc, known):
        if roman in NON_TARGET:
            continue
        if roman in EMPTY_ANNEXES:
            check_empty_annex(roman, block)
            continue
        if roman == "XIII":
            annex_rows = parse_annex_xiii(roman, block)
        elif roman == "XIV":
            annex_rows = parse_annex_xiv(roman, block)
        elif roman == "XV":
            annex_rows = parse_annex_xv(roman, block)
        else:
            annex_rows = parse_annex_xvi(roman, block)
        if not annex_rows:
            raise ParseError(f"{roman}: no entries extracted")
        rows.extend(annex_rows)
    return rows


@click.command(help="Parse consolidated Regulation 2017/1509 into a CSV candidate.")
@click.argument("celex")
@click.option(
    "--source",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    help="Parse these exact XHTML bytes instead of fetching from CELLAR.",
)
def main(celex: str, source: Path | None) -> None:
    try:
        check_registry(PROGRAM_KEY, [], ("Person", "LegalEntity", "Vessel"))
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
