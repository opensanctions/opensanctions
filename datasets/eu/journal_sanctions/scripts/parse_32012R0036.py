"""Parse consolidated Regulation (EU) 36/2012 (Syria) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation is a hybrid: one designation annex among goods annexes. The
consolidated text (post-2025, after the fall of the Assad government
triggered mass delistings) contains:

- Annex II — the Articles 14/15 fund-freeze list, parts A. Persons and
  B. Entities, each one five-column table (entry number, name, identifying
  information, reasons, date of listing) with heavy deletion markers and
  numbering gaps. Travel bans live in Decision 2013/255/CFSP.
- Annexes Ia, V, IX (equipment/goods/technology), III (competent-authority
  websites) and XI (cultural-goods categories) — no designations.

Five entries (A52, A68, A201, B64, B71) are printed fully redacted — every
cell holds only block characters — following court judgments; they carry no
transcribable content and are skipped under review pins.

Arabic name renderings are printed as embedded images: untranscribable, so
their empty "()" placeholders (and image-debris parentheticals such as
"(,)") are stripped from name lines. Token-level "(a.k.a. …)" annotations
printed mid-name ("Maher (a.k.a. Mahir) AL-ASSAD", 76 entries) stay whole
in the name for the crawler's review system — extracting them would
assemble names from non-contiguous pieces; only trailing alias groups
(a.k.a. lists and Arabic-text renderings) are peeled to aliases, split on
";" only.

Identifying-information lines end in a ";" line separator (stripped before
parsing) and are otherwise labelled. Deliberate drops, documented:
relational labels naming other parties (Relatives/associates lists,
directors, owners, registered agents), standalone kin lines ("Son of …",
"Wife of …"), and the passport-attribute "Issue No" line. Dates are
transcribed as the source prints them ("10.7.1969"); the crawler
normalizes dates.

Output: data/consolidated/32012R0036.csv (the EU Journal consolidated CSV
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
    cell_line,
    cell_lines,
    check_marker,
    clean,
    load_source,
    parse_dotted_date,
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

FRAMEWORK_CELEX = "32012R0036"
CONSOLIDATED_RE = re.compile(r"^02012R0036-\d{8}$")
PROGRAM_KEY = "EU-SYR"
# Annex II implements the Articles 14/15 fund freeze; travel bans live in
# Decision 2013/255/CFSP.
MEASURE = "Asset freeze"

NON_TARGET = frozenset({"Ia", "III", "V", "IX", "XI"})

SUBTITLE = (
    "LIST OF NATURAL AND LEGAL PERSONS, ENTITIES OR BODIES REFERRED TO IN "
    "ARTICLES 14, 15(1)(A) AND 15(1A)"
)
HEADER = (
    "",
    "Name",
    "Identifying information",
    "Reasons",
    "Date of listing",
)
# (part heading, part id, schema) in print order.
PARTS = (
    ("A. Persons", "A", "Person"),
    ("B. Entities", "B", "LegalEntity"),
)

NUMBER_RE = re.compile(r"^(\d+)\.$")
# Arabic renderings are printed as embedded images; their textual residue is
# an empty parenthetical (or comma-joined image debris, entry A125).
IMAGE_PARENS_RE = re.compile(r"\s*\(\s*[,;]?\s*\)")
# The label's separator is printed as a space, ":", or (one misprint,
# A260) ";".
AKA_RE = re.compile(r"^a\.k\.a\.?[:;]? (.+)$")
LATIN_RE = re.compile(r"[A-Za-z]")
# Info labels are printed as "Label: value"; relational label heads run up
# to 57 characters, beyond common.LABELLED_RE's cap.
LABEL_RE = re.compile(r"^([^:;]{1,70}): ?(.*)$")

# Entries printed fully redacted (every cell only block characters)
# following court judgments; no transcribable content.
REDACTED_ENTRIES = frozenset(
    {("A", "52"), ("A", "68"), ("A", "201"), ("B", "64"), ("B", "71")}
)
REDACTED_CELL_RE = re.compile(r"^[█\s(),;]*$")

# A176 prints a misprinted name (the opening paren of its last a.k.a. group
# is missing); the whole printed line stays in the name for review.
UNBALANCED_NAME_PINS = frozenset({("A", "176")})
# B78's a.k.a. group is misprinted without its closing paren, on its own
# line; the shed-one-unbalanced-paren rule applies (CAR precedent).
UNCLOSED_AKA_LINE_PINS = frozenset({("B", "78")})
# Entities printing their unlabeled legal-form-prefixed Cyrillic rendering
# as a bare second line; the printed list structure marks it as an alias.
ALIAS_LINE_PINS = frozenset({("B", "79"), ("B", "80"), ("B", "81")})

# One listing date is printed with a stray trailing period and two with a
# trailing semicolon; list punctuation, not date wording.
DATE_TRAILER_PINS = {("A", "354"): ";", ("A", "358"): ";", ("B", "94"): "."}

# Identifying-information labels → CSV column, exactly as printed.
INFO_LABELS = {
    "Date of birth": "birthDate",
    "DOB": "birthDate",
    "Place of birth": "birthPlace",
    "POB": "birthPlace",
    "Gender": "gender",
    "Nationality": "nationality",
    "Nationalities": "nationality",
    "Position": "position",
    "Position(s)": "position",
    "Rank": "position",
    "Function": "position",
    "Title": "position",
    "Address": "address",
    "Registered address": "address",
    "Headquarters": "address",
    "Suspected location": "address",
    "Place of registration": "address",
    "Principal place of business": "address",
    "Tel": "phone",
    "Tel/Fax": "phone",
    "Mobile": "phone",
    "Phone": "phone",
    "Phone Number": "phone",
    "Fax": "phone",
    "Email": "email",
    "E-mail": "email",
    "Website": "website",
    "Websites": "website",
    "Type of entity": "legalForm",
    "Date of registration": "incorporationDate",
    "Date of creation": "incorporationDate",
    "Incorporation date": "incorporationDate",
    "Registration number": "registrationNumber",
    "Incorporation number": "registrationNumber",
    "Syrian National ID Number": "idNumber",
    "National ID Number": "idNumber",
    "ID Number": "idNumber",
    "ID No": "idNumber",
    "National number": "idNumber",
    "Syrian national number": "idNumber",
    "National no": "idNumber",
    "UAE resident card": "idNumber",
    "Wagner group ID": "idNumber",
    "Passport no": "passportNumber",
    "Passport number": "passportNumber",
    "Syrian Passport": "passportNumber",
    "Turkish Passport number": "passportNumber",
    "Maiden name": "previousName",
    # A radio call sign is an alternative operational handle; the printed
    # label marks it as a (weak) alias-like identifier.
    "Call sign": "weakAlias",
    "Father’s Name": "fatherName",
    "Mother’s Name": "motherName",
    "Business sector": "sector",
}
# Free-text labels whose prose value goes to `notes`, label stripped. A
# value carrying a printed sub-label is decomposed under review below.
NOTES_LABELS = frozenset({"Other information", "Other identifying information"})
# Sub-labels observed inside notes-label values, mapped to their columns.
NOTES_SUBLABELS = {"Father": "fatherName"}
# Relational labels naming other parties: no CSV column, deliberately not
# transcribed.
DROP_LABELS = frozenset(
    {
        "Relatives/business associates/entities or partners/links",
        "Relatives/Associates",
        "Associated individuals",
        "Associated entities",
        "Business Associate",
        "Name of Director/Management",
        "Ultimate beneficial owner",
        "Director",
        "Registered agent",
        "General Manager",
        "Company owner",
    }
)
# Part-B info cells open with unlabeled address lines; "and"/"Or" lines
# connect alternative addresses within that leading block.
ADDRESS_CONNECTORS = frozenset({"and", "Or"})

# Reviewed hand-mappings for identifying-information lines the label rules
# cannot place, keyed by (part, entry) and the exact line (after the
# trailing-";" strip). An empty mapping drops the line deliberately. If the
# source line changes, the lookup misses and the run breaks for re-review.
INFO_OVERRIDES: dict[tuple[str, str], dict[str, tuple[tuple[str, str], ...]]] = {
    # Colon-less passport lines of the earliest entries.
    ("A", "1"): {"Diplomatic passport No D1903": (("passportNumber", "D1903"),)},
    ("A", "2"): {"Diplomatic passport No 4138": (("passportNumber", "4138"),)},
    ("A", "3"): {"Diplomatic passport No 983": (("passportNumber", "983"),)},
    ("A", "5"): {
        "Diplomatic passport No 014637352": (("passportNumber", "014637352"),)
    },
    ("A", "6"): {
        "Diplomatic passport No D000001300": (("passportNumber", "D000001300"),)
    },
    ("A", "8"): {
        "Passport No 000098044": (("passportNumber", "000098044"),),
        # A passport-attribute line (issue number of the document above);
        # document metadata with no CSV column.
        "Issue No 002-03-0015187": (),
    },
    ("A", "9"): {"Diplomatic passport No D0005788": (("passportNumber", "D0005788"),)},
    ("A", "10"): {
        "Former Head of Syrian Air Force Intelligence": (
            ("position", "Former Head of Syrian Air Force Intelligence"),
        ),
    },
    # The printed enumeration lists two passports.
    ("A", "13"): {
        "Passports No 86449 and No 842781": (
            ("passportNumber", "86449"),
            ("passportNumber", "842781"),
        ),
    },
    ("A", "18"): {"Passport No 002954347": (("passportNumber", "002954347"),)},
    ("A", "19"): {"Passport No N001820740": (("passportNumber", "N001820740"),)},
    # A corrigendum merged several labelled segments into one line.
    ("A", "45"): {
        (
            "Date of birth: 1951; Place of birth: Homs, Syria; Position: "
            "Deputy Chief of General Staff, Operations and Training, Syrian "
            "Army during the former al-Assad regime; Rank: "
            "Lieutenant-General, Syrian Arab Army; Gender: male"
        ): (
            ("birthDate", "1951"),
            ("birthPlace", "Homs, Syria"),
            (
                "position",
                "Deputy Chief of General Staff, Operations and Training, "
                "Syrian Army during the former al-Assad regime",
            ),
            ("position", "Lieutenant-General, Syrian Arab Army"),
            ("gender", "male"),
        ),
    },
    # A standalone kin line naming another person; relational, no column.
    ("A", "49"): {"Son of Ahmad Chehabi": ()},
    ("A", "50"): {
        "Date of birth: 2.6.1951; Place of birth: Homs, Syria; Gender: male": (
            ("birthDate", "2.6.1951"),
            ("birthPlace", "Homs, Syria"),
            ("gender", "male"),
        ),
    },
    # A colon-less birth-date line with approximation wording.
    ("A", "63"): {
        "Date of birth on or around 3.4.1973": (("birthDate", "on or around 3.4.1973"),)
    },
    # Validity wording inside the printed passport value stays whole.
    ("A", "72"): {
        "Passport No 707512830, expires 22.9.2020": (
            ("passportNumber", "707512830, expires 22.9.2020"),
        ),
    },
    ("A", "73"): {
        "Passport No (Syrian) 0000000914": (("passportNumber", "(Syrian) 0000000914"),)
    },
    ("A", "102"): {
        "Head of Deraa Regional Branch (General Security Directorate)": (
            (
                "position",
                "Head of Deraa Regional Branch (General Security Directorate)",
            ),
        ),
    },
    # A passport printed as a labelled block of dash-prefixed attribute
    # lines; the number is the identifier, the remaining lines are document
    # metadata with no CSV column.
    ("A", "212"): {
        "Syrian Passport:": (),
        "- number: 010312413": (("passportNumber", "010312413"),),
        "- issue number: 002‐15‐L093534": (),
        "- date of issue: 14.7.2015": (),
        "- place of issue: Damascus‐Centre": (),
        "- date of expiry: 13.7.2021": (),
    },
    # A standalone kin line naming other persons; relational, no column.
    ("A", "179"): {
        "Wife of Rami Makhlouf, daughter of Waleed (alias Walid) Othman": ()
    },
    # Relational lines naming the (designated) parent company.
    ("B", "10"): {
        "Subsidiary of Cham Holding (Sehanya Dara’a Highway, P.O. Box 9525)": ()
    },
    ("B", "11"): {
        "Subsidiary of Cham Holding (Sehanya Dara’a Highway, P.O. Box 9525)": ()
    },
    # A bare phone line under the address.
    ("B", "17"): {"+963 932 878282": (("phone", "+963 932 878282"),)},
    # Colon-less Tel./Fax lines.
    ("B", "24"): {
        ("Tel.+963 011 5810719; +963 11 4474579; +963 11 5810718; +963 11 5810719"): (
            ("phone", "+963 011 5810719"),
            ("phone", "+963 11 4474579"),
            ("phone", "+963 11 5810718"),
            ("phone", "+963 11 5810719"),
        ),
    },
    ("B", "25"): {
        "Tel. +963 11 5111352": (("phone", "+963 11 5111352"),),
        "Fax +963 11 5110117": (("phone", "+963 11 5110117"),),
    },
    ("B", "26"): {
        "Tel. + 96311 2121824; +963 11 2121825; +963 11 2131307": (
            ("phone", "+ 96311 2121824"),
            ("phone", "+963 11 2121825"),
            ("phone", "+963 11 2131307"),
        ),
    },
    # An unlabelled sector line, kept whole as the sector value.
    ("B", "62"): {"Sector of hydrocarbons": (("sector", "Sector of hydrocarbons"),)},
    # An empty-valued Other information heading over a list of the entity's
    # group companies; relational, not transcribed.
    ("B", "86"): {
        "Other information:": (),
        "Stroytransgaz Group,": (),
        "Stroytransgaz (STG) Logistic,": (),
        "Stroytransgaz (STG) Engineering,": (),
        "STG Engineering": (),
    },
    # An Other-information value whose printed sub-label marks the sector.
    ("B", "89"): {
        "Other information: Sector: Militia turned Security Company": (
            ("sector", "Militia turned Security Company"),
        ),
    },
}


def verbatim_date(part: str, record_id: str, text: str, ctx: str) -> str:
    trailer = DATE_TRAILER_PINS.get((part, record_id))
    if trailer is not None and text.endswith(trailer):
        text = text[: -len(trailer)]
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
    for schema_name in [part[2] for part in PARTS]:
        if model.get(schema_name) is None:
            raise ParseError(f"unknown schema {schema_name!r}")


def table_body(roman: str, table: Element, header: tuple[str, ...]) -> list[Element]:
    """Local copy of common.table_body: this document's tables end in one
    layout-artifact row (a single empty colspan cell), accepted here."""
    body: list[Element] = []
    rows = xpath_elements(table, ".//tr")
    if not rows:
        raise ParseError(f"{roman}: table has no rows")
    first = tuple(
        clean(element_text(td), roman) for td in xpath_elements(rows[0], "./td|./th")
    )
    if first != header:
        raise ParseError(f"{roman}: header {first} != expected {header}")
    for tr in rows[1:]:
        cells = xpath_elements(tr, "./td|./th")
        if len(cells) == 1:
            text = " ".join(element_text(cells[0]).split())
            if text == "":
                continue
            check_marker(text, roman)
            continue
        if len(cells) != len(header):
            raise ParseError(
                f"{roman}: row has {len(cells)} cells, expected {len(header)}"
            )
        body.append(tr)
    return body


def is_redacted(cells: list[Element], ctx: str) -> bool:
    texts = [" ".join(element_text(td).split()) for td in cells[1:]]
    redacted = ["█" in text for text in texts]
    if not any(redacted):
        return False
    if not all(REDACTED_CELL_RE.match(text) for text in texts):
        raise ParseError(f"{ctx}: partially redacted entry")
    return True


def strip_image_parens(line: str) -> str:
    return " ".join(IMAGE_PARENS_RE.sub("", line).split()).strip()


def alias_values(text: str) -> list[str]:
    values: list[str] = []
    for value in split_values(text):
        # Some alias lists repeat the printed label on every ";"-separated
        # item ("a.k.a. X; a.k.a. Y"); the label is list structure.
        repeated = AKA_RE.match(value)
        if repeated is not None:
            value = repeated.group(1)
        value = value.rstrip(",").strip()
        if value.startswith("‘") and value.endswith("’"):
            value = value[1:-1]
        if value:
            values.append(value)
    return values


def classify_group(ctx: str, group: str, row: Row) -> None:
    """Map one peeled trailing parenthetical group to aliases."""
    aka = AKA_RE.match(group)
    if aka is not None:
        row.add("alias", alias_values(aka.group(1)))
        return
    if not LATIN_RE.search(group):
        row.add("alias", alias_values(group))
        return
    raise ParseError(f"{ctx}: unrecognized name group {group[:60]!r}")


def peel_trailing_groups(ctx: str, line: str, row: Row) -> str:
    """Peel trailing alias groups off the name line, right to left."""
    while line.endswith(")"):
        depth = 0
        start = -1
        for index in range(len(line) - 1, -1, -1):
            char = line[index]
            if char == ")":
                depth += 1
            elif char == "(":
                depth -= 1
                if depth == 0:
                    start = index
                    break
        if start <= 0:
            break
        group = line[start + 1 : -1].strip()
        if AKA_RE.match(group) is None and LATIN_RE.search(group):
            break
        classify_group(ctx, group, row)
        line = line[:start].strip()
    return line


def check_name_parens(ctx: str, name: str) -> None:
    """Parentheticals that are not peelable trailing alias groups stay
    whole in the printed name for the crawler's review system — the
    document's token-level "(a.k.a. …)" annotations and entity acronyms
    ("(AMIF)"). Only unbalanced parens (misprints) must break for review."""
    depth = 0
    for char in name:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth < 0:
                raise ParseError(f"{ctx}: unbalanced parens in name {name[:60]!r}")
    if depth != 0:
        raise ParseError(f"{ctx}: unbalanced parens in name {name[:60]!r}")


def parse_name(ctx: str, part: str, record_id: str, td: Element, row: Row) -> None:
    lines = [strip_image_parens(line) for line in cell_lines(td, ctx)]
    lines = [line for line in lines if line]
    if not lines:
        raise ParseError(f"{ctx}: empty name cell")
    name = lines[0]
    if (part, record_id) in UNBALANCED_NAME_PINS:
        # The reviewed misprint stays whole in the name.
        row.add("name", [name])
    else:
        name = peel_trailing_groups(ctx, name, row)
        check_name_parens(ctx, name)
        row.add("name", [name])
    for line in lines[1:]:
        if line.startswith("(") and line.endswith(")"):
            classify_group(ctx, line[1:-1].strip(), row)
            continue
        aka = AKA_RE.match(line)
        if aka is not None:
            row.add("alias", alias_values(aka.group(1)))
            continue
        if not LATIN_RE.search(line):
            row.add("alias", alias_values(line))
            continue
        if (part, record_id) in UNCLOSED_AKA_LINE_PINS and line.startswith("(a.k.a. "):
            # The group's closing paren is misprinted away; shed the one
            # unbalanced opener (CAR precedent).
            row.add("alias", alias_values(line[len("(a.k.a. ") :]))
            continue
        if (part, record_id) in ALIAS_LINE_PINS:
            row.add("alias", [line])
            continue
        raise ParseError(f"{ctx}: unrecognized name line {line[:60]!r}")


def apply_notes_value(ctx: str, value: str, row: Row) -> None:
    """A notes-label value is bare prose; a printed sub-label is decomposed
    under review, and an unreviewed sub-label breaks the run."""
    sub = LABEL_RE.match(value)
    if sub is not None:
        column = NOTES_SUBLABELS.get(sub.group(1))
        if column is None:
            raise ParseError(f"{ctx}: unreviewed notes sub-label {value[:60]!r}")
        row.add(column, [sub.group(2)])
        return
    row.add("notes", [value])


def parse_info(ctx: str, part: str, record_id: str, td: Element, row: Row) -> None:
    overrides = INFO_OVERRIDES.get((part, record_id), {})
    # Part-B cells open with unlabeled address lines; "and"/"Or" connect
    # alternative addresses within that leading block.
    address_block = part == "B"
    pending_connector = False
    for raw in cell_lines(td, ctx):
        # Every line ends in a ";" separator; strip that scaffolding.
        line = raw[:-1].strip() if raw.endswith(";") else raw
        if line in overrides:
            for column, value in overrides[line]:
                row.add(column, [value])
            continue
        labelled = LABEL_RE.match(line)
        if labelled is not None:
            label, value = labelled.group(1), labelled.group(2)
            address_block = False
            if pending_connector:
                raise ParseError(f"{ctx}: address connector before {label!r}")
            if label in DROP_LABELS:
                continue
            if label in NOTES_LABELS:
                if value == "":
                    raise ParseError(f"{ctx}: label {label!r} without value")
                apply_notes_value(ctx, value, row)
                continue
            mapped = INFO_LABELS.get(label)
            if mapped is None:
                raise ParseError(f"{ctx}: unrecognized info label {label!r}")
            if value == "":
                raise ParseError(f"{ctx}: label {label!r} without value")
            values = split_values(value)
            for part_value in values[1:]:
                # A corrigendum once merged several labelled segments into
                # one line; an unreviewed merged line must break.
                if LABEL_RE.match(part_value) is not None:
                    raise ParseError(f"{ctx}: merged labelled line {line[:60]!r}")
            row.add(mapped, values)
            continue
        if address_block:
            if line in ADDRESS_CONNECTORS:
                pending_connector = True
                continue
            pending_connector = False
            row.add("address", [line])
            continue
        raise ParseError(f"{ctx}: unrecognized info line {line[:60]!r}")
    if pending_connector:
        raise ParseError(f"{ctx}: dangling address connector")


def parse_row(roman: str, part: str, schema: str, tr: Element) -> Row | None:
    cells = xpath_elements(tr, "./td|./th")
    ctx = f"{roman}.{part}"
    number = NUMBER_RE.match(cell_line(cells[0], ctx))
    if number is None:
        raise ParseError(f"{ctx}: unrecognized entry number cell")
    record_id = number.group(1)
    ctx = f"{ctx} entry {record_id}"
    if is_redacted(cells, ctx):
        if (part, record_id) not in REDACTED_ENTRIES:
            raise ParseError(f"{ctx}: unreviewed redacted entry")
        return None
    if (part, record_id) in REDACTED_ENTRIES:
        raise ParseError(f"{ctx}: pinned redacted entry has content")
    row = Row(annex_id(roman, part), schema, MEASURE, record_id=record_id)
    parse_name(ctx, part, record_id, cells[1], row)
    parse_info(ctx, part, record_id, cells[2], row)
    row.reason = " ".join(cell_lines(cells[3], ctx))
    row.start_date = verbatim_date(part, record_id, cell_line(cells[4], ctx), ctx)
    return row


def parse_part(roman: str, part_index: int, container: Element) -> list[Row]:
    heading, part, schema = PARTS[part_index]
    rows: list[Row] = []
    tables = 0
    seen_heading = False
    for child in container.iterchildren():
        if not isinstance(child.tag, str):
            continue
        cls = child.get("class") or ""
        if child.tag == "p" and cls == "modref":
            check_marker(" ".join(element_text(child).split()), roman)
            continue
        if child.tag == "p" and cls in SKIP_P_CLASSES:
            continue
        if child.tag == "p" and cls == "norm":
            text = clean(element_text(child), roman)
            if text != heading or seen_heading:
                raise ParseError(f"{roman}: unexpected part heading {text!r}")
            seen_heading = True
            continue
        if child.tag == "table":
            if not seen_heading:
                raise ParseError(f"{roman}: table before part heading")
            tables += 1
            for tr in table_body(f"{roman}.{part}", child, HEADER):
                row = parse_row(roman, part, schema, tr)
                if row is not None:
                    rows.append(row)
            continue
        raise ParseError(f"{roman}: unexpected <{child.tag} class={cls!r}> in part")
    if tables != 1:
        raise ParseError(f"{roman}: {tables} tables in part {part!r}, expected one")
    return rows


def parse_annex_ii(roman: str, block: Element) -> list[Row]:
    rows: list[Row] = []
    part_index = -1
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
            subtitle = clean(element_text(child), roman)
            if subtitle != SUBTITLE:
                raise ParseError(f"{roman}: unexpected subtitle {subtitle[:60]!r}")
            continue
        if child.tag == "div" and cls == "centered":
            part_index += 1
            if part_index >= len(PARTS):
                raise ParseError(f"{roman}: more part containers than parts")
            rows.extend(parse_part(roman, part_index, child))
            continue
        raise ParseError(f"{roman}: unexpected <{child.tag} class={cls!r}>")
    if part_index != len(PARTS) - 1:
        raise ParseError(f"{roman}: {part_index + 1} part containers, expected 2")
    return rows


def parse_document(doc: Element) -> list[Row]:
    rows: list[Row] = []
    for roman, block in annex_blocks(doc, {"II"} | NON_TARGET):
        if roman in NON_TARGET:
            continue
        annex_rows = parse_annex_ii(roman, block)
        if not annex_rows:
            raise ParseError(f"{roman}: no entries extracted")
        rows.extend(annex_rows)
    return rows


@click.command(help="Parse consolidated Regulation 36/2012 into a CSV candidate.")
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
            raise ParseError(f"not a consolidated 36/2012 CELEX: {celex!r}")
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
