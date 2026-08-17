"""Parse consolidated Regulation (EU) 267/2012 (Iran WMD) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation is a hybrid: two designation annexes among thirteen goods
and technology annexes. The designation annexes:

- Annex VIII — the Article 23(1) fund-freeze list (UN track): parts
  A. (sections "Natural persons" and "Entities", each a numbered list),
  B. and C. (entity lists). Entries are UN run-on paragraphs — a name
  sentence followed by "Label: value" sentences ("Function:", "DOB:",
  "Other information:", …) ending in "Date of UN designation:" or
  "Date of EU designation: D (UN: D2)". In parts B and C the name is
  terminated by a colon and followed by justification prose.
- Annex IX — the Article 23(2) fund-freeze list (EU track): parts I
  (nuclear/ballistic), II (IRGC) and III (IRISL), each printing an
  "A. Persons" table and a "B. Entities" table (part III prints the
  singular "A. Person"). Five-column tables; Farsi renderings and a.k.a.
  groups print as extra name-cell lines.

All other annexes list goods, technology, software or authority websites —
no designations. Travel bans live in Decision 2010/413/CFSP.

Document quirks pinned below: rows expunged in consolidation print as
"██████" blocks or as marker-empty rows and are skipped; one footnote row
(time-limited application per Implementing Regulation (EU) 2016/603) is
accepted and not transcribed; listing dates carry designation-history
parentheticals — "(D, suspended)" JCPOA suspensions and "(UN: D)" — which
are stripped per the contract's date-only startDate rule; sub-entries
print numbers like "5. (a)" and keep them as their record identifier.
Dates are transcribed as the source prints them; the crawler normalizes.

Output: data/consolidated/32012R0267.csv (the EU Journal consolidated CSV
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
    MARKER_ROW_RE,
    SKIP_P_CLASSES,
    ParseError,
    Row,
    annex_blocks,
    check_consolidated_celex,
    check_marker,
    check_registry,
    clean,
    load_source,
    split_values,
    summary,
    to_record,
    validate_records,
    verbatim_date,
    write_csv,
)
from lxml import html
from zavod.helpers.html import element_text, xpath_elements
from zavod.util import Element

FRAMEWORK_CELEX = "32012R0267"
PROGRAM_KEY = "EU-IRN"
# Annexes VIII and IX implement the Article 23 fund freezes; travel bans
# live in Decision 2010/413/CFSP.
MEASURE = "Asset freeze"

NON_TARGET = frozenset(
    {
        "I",
        "II",
        "IIa",
        "III",
        "IV",
        "IVa",
        "V",
        "VI",
        "VIa",
        "VIb",
        "VII",
        "VIIa",
        "VIIb",
        "X",
    }
)

# --- Annex VIII (UN track) ---------------------------------------------------

# Part letter → (heading prefix, sections). Part A holds two numbered
# sections whose printed headings drive the schema; parts B and C list
# entities only.
VIII_PART_HEADINGS = {
    "A.": "Persons and entities involved in nuclear or ballistic missiles activities",
    "B.": (
        "Entities owned, controlled, or acting on behalf of the "
        "Iranian Revolutionary Guard Corps"
    ),
    "C.": (
        "Entities owned, controlled, or acting on behalf of the "
        "Islamic Republic of Iran Shipping Lines"
    ),
}
VIII_SECTIONS = {
    "Natural persons": ("VIII.A.PERSONS", "Person"),
    "Entities": ("VIII.A.ENTITIES", "LegalEntity"),
}

# Labels sliced out of the run-on entry paragraphs, in the printed
# spellings only. Values map to columns; the date labels feed startDate
# and "Other information" becomes bare notes prose.
VIII_FIELD_COLUMNS = {
    "Function": "position",
    "Title": "position",
    "DOB": "birthDate",
    "POB": "birthPlace",
    "Nationality": "nationality",
    "Passport no": "passportNumber",
    "National identification no": "idNumber",
    "Address": "address",
    "Location": "address",
    "A.k.a.": "alias",
    "A.K.A.": "alias",
}
VIII_NOTES_LABEL = "Other information"
VIII_UN_DATE_LABEL = "Date of UN designation"
VIII_EU_DATE_LABEL = "Date of EU designation"
VIII_LABELS = tuple(VIII_FIELD_COLUMNS) + (
    VIII_NOTES_LABEL,
    VIII_UN_DATE_LABEL,
    VIII_EU_DATE_LABEL,
)
# Printed label tokens, longest first: the ":"-suffixed form for every
# label, plus the colon-less "A.k.a. " variant this document also prints.
VIII_LABEL_TOKENS = tuple(
    sorted(
        [(f"{label}:", label) for label in VIII_LABELS] + [("A.k.a. ", "A.k.a.")],
        key=lambda pair: -len(pair[0]),
    )
)

VIII_NUMBER_RE = re.compile(r"^\((\d+)\)$")
# A label slice starts the text or follows whitespace, at parenthesis
# depth zero — the document also glues labels onto the preceding value
# with no punctuation ("… Tehran, Iran A.K.A.: 3MG Date of EU …").
VIII_BOUNDARIES = (" ",)
# Trailing name parenthetical carrying printed alias labels.
VIII_ALIAS_TAIL_RE = re.compile(r"^(.+?) \(alias(?:es)?:? (.+)\)$")
# Free-prose entity entries open with the name and repeat it as the
# subject of the justification prose.
VIII_NAME_REPEAT_RE = re.compile(r"^(.+?)\. (\1[ ,].*)$", re.S)
# Reviewed heads whose inner period is part of the printed name (an
# initial or corporate abbreviation), not a sentence stop.
VIII_DOTTED_NAME_PINS = frozenset({("VIII.A.PERSONS", "19")})
# Reviewed name/prose decompositions for heads the repetition rule cannot
# split (the prose restates the name with different punctuation). Keyed on
# the exact printed head; a changed head breaks for re-review.
VIII_HEAD_OVERRIDES: dict[tuple[str, str], tuple[str, str, str]] = {
    ("VIII.A.ENTITIES", "18"): (
        "First East Export Bank, P.L.C. First East Export Bank, PLC is owned "
        "or controlled by, or acts on behalf of, Bank Mellat. Over the last "
        "seven years, Bank Mellat has facilitated hundreds of millions of "
        "dollars in transactions for Iranian nuclear, missile, and defense "
        "entities.",
        "First East Export Bank, P.L.C.",
        "First East Export Bank, PLC is owned or controlled by, or acts on "
        "behalf of, Bank Mellat. Over the last seven years, Bank Mellat has "
        "facilitated hundreds of millions of dollars in transactions for "
        "Iranian nuclear, missile, and defense entities.",
    ),
    ("VIII.A.ENTITIES", "28"): (
        "Malek Ashtar University. A subordinate of the DTRSC within MODAFL. "
        "This includes research groups previously falling under the Physics "
        "Research Center (PHRC). IAEA inspectors have not been allowed to "
        "interview staff or see documents under the control of this "
        "organization to resolve the outstanding issue of the possible "
        "military dimension to Iran’s nuclear program.",
        "Malek Ashtar University",
        "A subordinate of the DTRSC within MODAFL. This includes research "
        "groups previously falling under the Physics Research Center (PHRC). "
        "IAEA inspectors have not been allowed to interview staff or see "
        "documents under the control of this organization to resolve the "
        "outstanding issue of the possible military dimension to Iran’s "
        "nuclear program.",
    ),
    ("VIII.A.ENTITIES", "33"): (
        "Nuclear Research Center for Agriculture and Medicine. The Nuclear "
        "Research Center for Agriculture and Medicine (NFRPC) is a large "
        "research component of the Atomic Energy Organization of Iran "
        "(AEOI), which was designated in resolution 1737 (2006). The NFRPC "
        "is AEOI’s center for the development of nuclear fuel and is "
        "involved in enrichment-related activities.",
        "Nuclear Research Center for Agriculture and Medicine",
        "The Nuclear Research Center for Agriculture and Medicine (NFRPC) is "
        "a large research component of the Atomic Energy Organization of "
        "Iran (AEOI), which was designated in resolution 1737 (2006). The "
        "NFRPC is AEOI’s center for the development of nuclear fuel and is "
        "involved in enrichment-related activities.",
    ),
    ("VIII.A.ENTITIES", "42"): (
        "Sabalan Company. Sabalan is a cover name for SHIG.",
        "Sabalan Company",
        "Sabalan is a cover name for SHIG.",
    ),
    ("VIII.A.ENTITIES", "46"): (
        "Sahand Aluminum Parts Industrial Company (SAPICO). SAPICO is a "
        "cover name for SHIG.",
        "Sahand Aluminum Parts Industrial Company (SAPICO)",
        "SAPICO is a cover name for SHIG.",
    ),
    # The prose restates the name with a corrected spelling (the printed
    # name says "Satarri"); the name column keeps the designation's form.
    ("VIII.A.ENTITIES", "50"): (
        "Shahid Satarri Industries. Shahid Sattari Industries is owned or "
        "controlled by, or acts on behalf of, SBIG.",
        "Shahid Satarri Industries",
        "Shahid Sattari Industries is owned or controlled by, or acts on "
        "behalf of, SBIG.",
    ),
}
# Source misprints in designation dates, repaired under review by exact
# printed value (a stray space inside the date). A changed value misses
# the lookup and the run breaks for re-review.
VIII_DATE_MISPRINTS = {"18.4. 2012": "18.4.2012"}

# Location list items that are not addresses, reviewed by exact value:
# a printed identifier keeps its issuing-country parenthetical whole.
VIII_LOCATION_ITEM_OVERRIDES = {
    "V.A.T. Number BE480224531 (Belgium)": (
        "taxNumber",
        "BE480224531 (Belgium)",
    ),
    "Business Registration Number LL06889 (Malaysia)": (
        "registrationNumber",
        "LL06889 (Malaysia)",
    ),
}

# --- Annex IX (EU track) -----------------------------------------------------

IX_PART_HEADINGS = {
    "I.": ("Persons and entities involved in nuclear or ballistic missile activities"),
    "II.": "Iranian Revolutionary Guard Corps (IRGC)",
    "III.": "Islamic Republic of Iran Shipping Lines (IRISL)",
}
# The printed table sub-heading; part III prints the singular "A. Person".
IX_SECTION_RE = re.compile(r"^(A)\. Persons?$|^(B)\. Entities$")
IX_SECTION_SCHEMAS = {"A": "Person", "B": "LegalEntity"}
IX_HEADER = ("", "Name", "Identifying information", "Reasons", "Date of listing")

IX_NUMBER_RE = re.compile(r"^(\d+)\.(?: \(([a-z]{1,2})\))?$")
# Listing dates print designation-history parentheticals — JCPOA
# suspensions "(23.1.2016, suspended)", "(23.4.2007, suspended since
# 16.1.2016)" and "(UN: 9.6.2010)" — inline or on their own cell line;
# stripped per the contract's date-only startDate rule.
IX_HISTORY = (
    r"\((?:UN: )?\d{1,2}\.\d{1,2}\.\d{4}"
    r"(?:, suspended(?: since \d{1,2}\.\d{1,2}\.\d{4})?)?\)"
)
IX_DATE_RE = re.compile(r"^(\d{1,2}\.\d{1,2}\.\d{4})(?: " + IX_HISTORY + r")?$")
IX_HISTORY_RE = re.compile(r"^" + IX_HISTORY + r"$")
AKA_LINE_RE = re.compile(r"^a\.k\.a\.?:?\s+(.+)$")
AKA_ITEM_SPLIT_RE = re.compile(r",? a\.k\.a\.?:?\s+")
FARSI_RE = re.compile(r"^Farsi:\s*(.*)$")
LATIN_RE = re.compile(r"[A-Za-z]")

# One footnote row states a time-limited application; it annotates the
# designation's life, has no contract column, and is not transcribed.
IX_FOOTNOTE = (
    "In accordance with Council Implementing Regulation (EU) 2016/603, "
    "this entry shall apply until 22 October 2016."
)

IX_INFO_LABELS = {
    "DOB": "birthDate",
    "POB": "birthPlace",
    "Gender": "gender",
    "Nationality": "nationality",
    "Function": "position",
    "Rank": "position",
    "Passport no.": "passportNumber",
    "National ID": "idNumber",
    "National ID no.": "idNumber",
    "Address": "address",
    "Adrdress": "address",
    "Address (2)": "address",
    "Address 1": "address",
    "Address 2": "address",
    "Address 2 (factory)": "address",
    "Address 3": "address",
    "Address no. 1": "address",
    "Address no. 2": "address",
    "Address no. 3": "address",
    "Address no. 4": "address",
    "Address no. 5": "address",
    "Address no. 6": "address",
    "Address no. 7": "address",
    "Address no. 8": "address",
    "Address No 1": "address",
    "Address of NRC": "address",
    "Last address known": "address",
    "Postal address": "address",
    "P.O. Box": "address",
    "Head Office": "address",
    "Complex": "address",
    "Place of registration": "address",
    "Principal place of business": "address",
    "Registration number": "registrationNumber",
    "Registration Number": "registrationNumber",
    "Registration no.": "registrationNumber",
    "Business Registration Number": "registrationNumber",
    "Company Number": "registrationNumber",
    "Date of registration": "incorporationDate",
    "Type of entity": "legalForm",
    "VAT No": "taxNumber",
    "Economic code": "taxNumber",
    "SWIFT/BIC": "swiftBic",
    "Tel.": "phone",
    "Tel": "phone",
    "Telephone": "phone",
    "Telephone no.": "phone",
    "Phone": "phone",
    "Fax": "phone",
    "Email": "email",
    "EMail": "email",
    "Website": "website",
    "Web Site": "website",
    "Web": "website",
    "Entity Web Site": "website",
}
# Relational labels naming other parties: no CSV column, deliberately not
# transcribed. The label line and its bare continuation lines are consumed.
IX_DROP_LABELS = frozenset(
    {
        "Associated entities",
        "Associated entity",
        "Associated individual",
        "Associated individuals",
        "Other associated entities (subsidiaries)",
    }
)
# Columns whose labelled value legitimately continues onto bare follow-on
# lines in this document (address lists, e-mail lists). Bare lines after
# any other label are new structure.
IX_CONTINUABLE = frozenset({"address", "email"})

# The Bank Melli name wraps across two printed lines (trailing comma).
WRAPPED_NAME_PINS = frozenset({("IX.I.B", "5")})
# Reviewed hand-mappings for whole name cells the grammar cannot place,
# keyed by (annex, entry) with the exact printed lines. A changed line
# misses the lookup and the run breaks for re-review.
NAME_OVERRIDES: dict[
    tuple[str, str], dict[tuple[str, ...], tuple[tuple[str, str], ...]]
] = {
    # The parenthetical mixes an unlabelled long form with a.k.a. items,
    # wrapping across three lines.
    ("IX.I.B", "99"): {
        (
            "TABA (Iran Cutting Tools Manufacturing company - Taba Towlid "
            "Abzar Boreshi Iran;",
            "a.k.a. Iran Centrifuge Technology Co.; Iran's Centrifuge "
            "Technology Company; Sherkate Technology Centrifuge",
            "Iran, TESA, TSA)",
        ): (
            ("name", "TABA"),
            (
                "alias",
                "Iran Cutting Tools Manufacturing company - Taba Towlid "
                "Abzar Boreshi Iran",
            ),
            ("alias", "Iran Centrifuge Technology Co."),
            ("alias", "Iran's Centrifuge Technology Company"),
            ("alias", "Sherkate Technology Centrifuge Iran, TESA, TSA"),
        ),
    },
}
# Telephone lines also print without a colon ("Tel. +98 21 …", "Fax +375
# …"); the value must look like a number for the label to bind.
IX_PHONE_RE = re.compile(r"^(?:Tel\.?|Fax)\s+(\+?[\d(].*)$")

# Reviewed hand-mappings for identifying-information lines, keyed by
# (annex, entry) and the exact line; an empty mapping drops the line.
IX_INFO_OVERRIDES: dict[tuple[str, str], dict[str, tuple[tuple[str, str], ...]]] = {
    # A colon-less registration line; the identifier keeps its printed
    # issuance clause whole.
    ("IX.III.B", "158"): {
        "Business registration document # 5478431, issued March 2009": (
            ("registrationNumber", "5478431, issued March 2009"),
        ),
        # An IMO company number on a legal person; imoNumber is a
        # Vessel-only property, so the company identifier lands in
        # registrationNumber and the prose tail is bare notes.
        "IMO number: 5878431; established in 2009": (
            ("registrationNumber", "5878431"),
            ("notes", "established in 2009"),
        ),
    },
    # A colon-less registration-date line.
    ("IX.I.B", "147"): {
        "Date of registration 18.3.1992": (("incorporationDate", "18.3.1992"),),
    },
}


# Only the dotted and worded forms occur in this document.
DATE_FORMATS = (
    "dotted",
    "worded",
)


# --- shared helpers ----------------------------------------------------------


def cell_texts(td: Element, ctx: str) -> list[str]:
    """The cell's non-empty <p> lines; in-cell modification markers are
    verified and skipped (this document prints them inside cells)."""
    lines: list[str] = []
    for p in xpath_elements(td, ".//p"):
        raw = " ".join(element_text(p).split())
        if not raw:
            continue
        if MARKER_ROW_RE.match(raw):
            continue
        line = clean(raw, ctx)
        if line:
            lines.append(line)
    return lines


def split_lettered(ctx: str, value: str) -> list[str]:
    """Split a UN "(a) … (b) …" enumeration in sequence-checked order."""
    if not value.startswith("(a) "):
        return [value]
    items: list[str] = []
    positions: list[int] = []
    letter = ord("a")
    pos = 0
    while True:
        marker = f"({chr(letter)}) "
        found = value.find(marker, pos)
        if found == -1:
            break
        positions.append(found)
        pos = found + len(marker)
        letter += 1
    # A single leading "(a)" occurs in the document (the list's other
    # items were removed); the marker is list scaffolding either way.
    bounds = positions + [len(value)]
    for index, start in enumerate(positions):
        item = value[start + 4 : bounds[index + 1]]
        item = item.strip().rstrip(";").rstrip(",").strip()
        if item:
            items.append(item)
    return items


def strip_period(value: str) -> str:
    """Drop the run-on sentence stop from a structured value."""
    return value[:-1].strip() if value.endswith(".") else value


# --- Annex VIII --------------------------------------------------------------


def viii_segments(ctx: str, text: str) -> tuple[str, list[tuple[str, str]]]:
    """Slice the run-on paragraph at known labels on sentence boundaries."""
    hits: list[tuple[int, str, int]] = []
    depth = 0
    for index, char in enumerate(text):
        if char == "(":
            depth += 1
        elif char == ")":
            depth = max(0, depth - 1)
        if depth != 0:
            continue
        for token, label in VIII_LABEL_TOKENS:
            if not text.startswith(token, index):
                continue
            if index != 0 and not any(
                text.endswith(boundary, 0, index) for boundary in VIII_BOUNDARIES
            ):
                continue
            hits.append((index, label, len(token)))
            break
    hits.sort()
    if not hits:
        raise ParseError(f"{ctx}: no labels in entry {text[:60]!r}")
    head = text[: hits[0][0]].strip()
    segments: list[tuple[str, str]] = []
    for pos, (index, label, width) in enumerate(hits):
        end = hits[pos + 1][0] if pos + 1 < len(hits) else len(text)
        value = text[index + width : end].strip()
        segments.append((label, value))
    return head, segments


def viii_start_date(ctx: str, row: Row, label: str, value: str) -> None:
    # Either date label may carry the other track's date as a
    # designation-history parenthetical — "(UN: D)" / "(EU: D)" — which is
    # stripped per the contract's date-only startDate rule.
    if row.start_date:
        raise ParseError(f"{ctx}: second designation date")
    value = strip_period(value)
    match = re.match(r"^(.+?) \((?:UN|EU): [^)]+\)$", value)
    if match is not None:
        value = match.group(1)
    value = VIII_DATE_MISPRINTS.get(value, value)
    row.start_date = verbatim_date(value, ctx, DATE_FORMATS)


def viii_head(
    ctx: str, annex: str, record_id: str, head: str, row: Row, entity: bool
) -> None:
    """Set the entry's name from the pre-label text; free prose after the
    name sentence in entity entries is the designation justification."""
    override = VIII_HEAD_OVERRIDES.get((annex, record_id))
    if override is not None:
        expected, name, reason = override
        if head != expected:
            raise ParseError(f"{ctx}: overridden head changed")
        row.add("name", [name])
        row.reason = reason
        return
    if entity:
        repeat = VIII_NAME_REPEAT_RE.match(head)
        if repeat is not None:
            row.add("name", [repeat.group(1)])
            row.reason = repeat.group(2)
            return
    name = strip_period(head)
    alias_tail = VIII_ALIAS_TAIL_RE.match(name)
    if alias_tail is not None:
        name, alias_group = alias_tail.groups()
        row.add("alias", split_values(alias_group))
    if ". " in name and (annex, record_id) not in VIII_DOTTED_NAME_PINS:
        raise ParseError(f"{ctx}: unsplit head prose {head[:80]!r}")
    row.add("name", [name])


def parse_viii_entry(
    annex: str, schema: str, record_id: str, text: str, colon_name: bool
) -> Row:
    ctx = f"{annex} entry {record_id}"
    row = Row(annex, schema, MEASURE, record_id=record_id)
    if colon_name:
        name, _, rest = text.partition(": ")
        if not rest:
            raise ParseError(f"{ctx}: no colon after name {text[:60]!r}")
        row.add("name", [name])
        head, segments = viii_segments(ctx, rest)
        if head:
            row.reason = head
    else:
        head, segments = viii_segments(ctx, text)
        viii_head(ctx, annex, record_id, head, row, entity=schema != "Person")
    for label, value in segments:
        if label in (VIII_UN_DATE_LABEL, VIII_EU_DATE_LABEL):
            viii_start_date(ctx, row, label, value)
            continue
        if label == VIII_NOTES_LABEL:
            for item in split_lettered(ctx, value):
                row.add("notes", [item])
            continue
        column = VIII_FIELD_COLUMNS[label]
        value = strip_period(value)
        items: list[str] = []
        for lettered in split_lettered(ctx, value):
            items.extend(split_values(lettered))
        if column == "address":
            mapped: list[str] = []
            for item in items:
                override = VIII_LOCATION_ITEM_OVERRIDES.get(item)
                if override is not None:
                    row.add(override[0], [override[1]])
                else:
                    mapped.append(item)
            items = mapped
        row.add(column, items)
    if not row.start_date:
        raise ParseError(f"{ctx}: no designation date")
    return row


def viii_entry_grids(ctx: str, container: Element) -> list[tuple[str, str]]:
    """(number, text) for every entry grid directly inside the container."""
    entries: list[tuple[str, str]] = []
    for grid in xpath_elements(container, "./div[contains(@class, 'grid-container')]"):
        col1 = xpath_elements(
            grid, "./div[contains(@class, 'grid-list-column-1')]", expect_exactly=1
        )[0]
        col2 = xpath_elements(
            grid, "./div[contains(@class, 'grid-list-column-2')]", expect_exactly=1
        )[0]
        number = clean(element_text(col1), ctx)
        match = VIII_NUMBER_RE.match(number)
        if match is None:
            raise ParseError(f"{ctx}: unrecognized entry number {number!r}")
        entries.append((match.group(1), clean(element_text(col2), ctx)))
    return entries


def check_increasing(ctx: str, numbers: list[str]) -> None:
    values = [int(number) for number in numbers]
    if values != sorted(set(values)):
        raise ParseError(f"{ctx}: entry numbers not increasing")


def parse_viii_part_a(col2: Element) -> list[Row]:
    rows: list[Row] = []
    section: tuple[str, str] | None = None
    seen_heading = False
    for child in col2:
        cls = child.get("class") or ""
        if child.tag == "p" and cls == "norm":
            text = clean(element_text(child), "VIII.A")
            if not text.startswith(VIII_PART_HEADINGS["A."]) or seen_heading:
                raise ParseError(f"VIII.A: unexpected heading {text[:60]!r}")
            seen_heading = True
            continue
        if child.tag == "p" and cls == "modref":
            check_marker(" ".join(element_text(child).split()), "VIII.A")
            continue
        if child.tag == "div" and cls == "list":
            grids = xpath_elements(child, "./div[contains(@class, 'grid-container')]")
            if not grids:
                heading = clean(element_text(child), "VIII.A")
                if heading not in VIII_SECTIONS:
                    raise ParseError(f"VIII.A: unknown section {heading!r}")
                section = VIII_SECTIONS[heading]
                continue
            if section is None:
                raise ParseError("VIII.A: entries before a section heading")
            annex, schema = section
            entries = viii_entry_grids(annex, child)
            check_increasing(annex, [number for number, _ in entries])
            for number, text in entries:
                rows.append(
                    parse_viii_entry(annex, schema, number, text, colon_name=False)
                )
            continue
        raise ParseError(f"VIII.A: unexpected <{child.tag} class={cls!r}>")
    return rows


def parse_viii_part_entities(part: str, col2: Element) -> list[Row]:
    annex = f"VIII.{part.rstrip('.')}"
    rows: list[Row] = []
    seen_heading = False
    numbers: list[str] = []
    for child in col2:
        cls = child.get("class") or ""
        if child.tag == "p" and cls == "norm":
            text = clean(element_text(child), annex)
            if not text.startswith(VIII_PART_HEADINGS[part]) or seen_heading:
                raise ParseError(f"{annex}: unexpected heading {text[:60]!r}")
            seen_heading = True
            continue
        if child.tag == "p" and cls == "modref":
            check_marker(" ".join(element_text(child).split()), annex)
            continue
        if child.tag == "div" and "grid-container" in cls:
            for number, text in viii_entry_grids(annex, _wrap(child)):
                numbers.append(number)
                rows.append(
                    parse_viii_entry(
                        annex, "LegalEntity", number, text, colon_name=True
                    )
                )
            continue
        raise ParseError(f"{annex}: unexpected <{child.tag} class={cls!r}>")
    check_increasing(annex, numbers)
    return rows


def _wrap(grid: Element) -> Element:
    """A single-entry container for viii_entry_grids."""
    parent = grid.makeelement("div", {})
    parent.append(grid)
    return parent


def parse_annex_viii(block: Element) -> list[Row]:
    rows: list[Row] = []
    parts = xpath_elements(block, "./div[contains(@class, 'grid-container')]")
    if len(parts) != 3:
        raise ParseError(f"VIII: {len(parts)} part containers, expected 3")
    for part, grid in zip(("A.", "B.", "C."), parts, strict=True):
        col1 = xpath_elements(
            grid, "./div[contains(@class, 'grid-list-column-1')]", expect_exactly=1
        )[0]
        letter = clean(element_text(col1), "VIII")
        if letter != part:
            raise ParseError(f"VIII: part {letter!r}, expected {part!r}")
        col2 = xpath_elements(
            grid, "./div[contains(@class, 'grid-list-column-2')]", expect_exactly=1
        )[0]
        if part == "A.":
            rows.extend(parse_viii_part_a(col2))
        else:
            rows.extend(parse_viii_part_entities(part, col2))
    return rows


# --- Annex IX ----------------------------------------------------------------


def split_aka_items(value: str) -> list[str]:
    """Split an alias enumeration on ";" and on repeated printed a.k.a.
    labels; comma-joined pieces without their own label stay whole."""
    items: list[str] = []
    for piece in value.split(";"):
        piece = piece.strip().rstrip(",").strip()
        if not piece:
            continue
        piece = re.sub(r"^a\.k\.a\.?:?\s+", "", piece)
        for item in AKA_ITEM_SPLIT_RE.split(piece):
            item = item.strip().rstrip(",").strip()
            if item:
                items.append(item)
    return items


AKA_GROUP_RE = re.compile(r"^(?:a\.k\.a|alias)|[,;] ?a\.k\.a")


def peel_trailing_aka(name: str, row: Row) -> str:
    """Peel a trailing balanced "(… a.k.a. …)" parenthetical to aliases;
    unlabelled parentheticals stay part of the printed name."""
    if not name.endswith(")"):
        return name
    depth = 0
    start = -1
    for index in range(len(name) - 1, -1, -1):
        if name[index] == ")":
            depth += 1
        elif name[index] == "(":
            depth -= 1
            if depth == 0:
                start = index
                break
    if start <= 0:
        return name
    inner = name[start + 1 : -1].strip()
    if AKA_GROUP_RE.search(inner) is None:
        return name
    row.add("alias", split_aka_items(inner))
    return name[:start].strip().rstrip(",;").strip()


def parse_ix_name(
    ctx: str, annex: str, record_id: str, lines: list[str], row: Row, images: int
) -> None:
    if not lines:
        raise ParseError(f"{ctx}: empty name cell")
    override = NAME_OVERRIDES.get((annex, record_id))
    if override is not None:
        mapped = override.get(tuple(lines))
        if mapped is None:
            raise ParseError(f"{ctx}: overridden name cell changed")
        for column, value in mapped:
            row.add(column, [value])
        return
    name = lines[0]
    rest = lines[1:]
    if name.endswith(","):
        if (annex, record_id) not in WRAPPED_NAME_PINS:
            raise ParseError(f"{ctx}: unpinned wrapped name {name[:60]!r}")
        if not rest:
            raise ParseError(f"{ctx}: wrapped name without continuation")
        name = f"{name} {rest.pop(0)}"
    name = peel_trailing_aka(name, row)
    group: str | None = None
    if "(" in name and name.count("(") > name.count(")"):
        prefix, _, remainder = name.partition(" (")
        if not remainder.startswith(("a.k.a", "alias")):
            raise ParseError(f"{ctx}: unclosed name parenthetical {name[:60]!r}")
        name = prefix
        group = remainder
    row.add("name", [name])
    for line in rest:
        if group is not None:
            group = f"{group} {line}"
            if line.endswith(")"):
                row.add("alias", split_aka_items(group[:-1].rstrip(")")))
                group = None
            continue
        if line.startswith("(") and line.endswith(")"):
            inner = line[1:-1].strip()
            if inner.startswith(("a.k.a", "alias")):
                row.add("alias", split_aka_items(inner))
            else:
                row.add("alias", [inner])
            continue
        if line.startswith("(") and not line.endswith(")"):
            inner = line[1:].strip()
            if not inner.startswith(("a.k.a", "alias")):
                raise ParseError(f"{ctx}: unclosed parenthetical {line[:60]!r}")
            group = inner
            continue
        aka = AKA_LINE_RE.match(line)
        if aka is not None:
            row.add("alias", split_aka_items(aka.group(1)))
            continue
        farsi = FARSI_RE.match(line)
        if farsi is not None:
            if farsi.group(1):
                row.add("alias", [farsi.group(1)])
            elif images == 0:
                # An empty label is legal only when the rendering is an
                # embedded image — untranscribable, so no alias results.
                raise ParseError(f"{ctx}: empty Farsi rendering")
            continue
        if not LATIN_RE.search(line):
            row.add("alias", [line])
            continue
        raise ParseError(f"{ctx}: unrecognized name line {line[:60]!r}")
    if group is not None:
        raise ParseError(f"{ctx}: unterminated alias group")


def parse_ix_info(
    ctx: str, annex: str, record_id: str, td: Element, row: Row, entity: bool
) -> None:
    overrides = IX_INFO_OVERRIDES.get((annex, record_id), {})
    # Entity cells conventionally open with unlabelled address lines
    # (52 cells in the document); person cells always label their fields.
    block: str | None = "address" if entity else None
    opened = entity
    dropped = False
    for line in cell_texts(td, ctx):
        if line in overrides:
            for column, value in overrides[line]:
                row.add(column, [value])
            block, opened, dropped = None, False, False
            continue
        labelled = LABELLED_RE.match(line)
        label = labelled.group(1) if labelled is not None else None
        if label in IX_DROP_LABELS:
            block, opened, dropped = None, False, True
            continue
        phone = IX_PHONE_RE.match(line)
        if phone is not None:
            row.add("phone", split_values(phone.group(1)))
            block, opened, dropped = "phone", phone.group(1).endswith(";"), False
            continue
        if label is not None and label in IX_INFO_LABELS:
            assert labelled is not None
            column = IX_INFO_LABELS[label]
            value = labelled.group(2)
            if value:
                row.add(column, split_values(value))
            # An empty-valued label or a value ending in ";" holds further
            # values on the following bare lines.
            block, dropped = column, False
            opened = not value or value.endswith(";")
            continue
        if dropped:
            continue
        if block is not None and (opened or block in IX_CONTINUABLE):
            row.add(block, split_values(line))
            continue
        raise ParseError(f"{ctx}: unrecognized info line {line[:60]!r}")


def ix_record_id(ctx: str, cell: Element) -> str:
    text = clean(element_text(cell), ctx)
    match = IX_NUMBER_RE.match(text)
    if match is None:
        raise ParseError(f"{ctx}: unrecognized entry number {text!r}")
    number, letter = match.groups()
    return f"{number} ({letter})" if letter else number


def is_expunged(texts: list[str]) -> bool:
    """A row whose printed content was expunged in consolidation: black
    blocks, empty marker remnants, or deletion dashes only — sub-entry
    letters and entry numbers may survive next to the blocks."""
    joined = "".join(texts)
    if not joined.strip("— "):
        return True
    if "█" in joined:
        residue = [re.sub(r"\([a-z0-9]{1,2}\)|\d+\.", "", t) for t in texts]
        return all(not t or set(t) <= set("█(), —") for t in residue)
    return False


def parse_ix_table(annex: str, schema: str, table: Element) -> list[Row]:
    rows: list[Row] = []
    trs = xpath_elements(table, ".//tr")
    if not trs:
        raise ParseError(f"{annex}: table has no rows")
    header = tuple(
        clean(element_text(td), annex) for td in xpath_elements(trs[0], "./td|./th")
    )
    if header != IX_HEADER:
        raise ParseError(f"{annex}: header {header} != expected {IX_HEADER}")
    last: tuple[int, str] = (0, "")
    for tr in trs[1:]:
        cells = xpath_elements(tr, "./td|./th")
        if len(cells) == 1:
            raw = " ".join(element_text(cells[0]).split())
            if not raw:
                continue
            if MARKER_ROW_RE.match(raw):
                continue
            joined = " ".join(cell_texts(cells[0], annex))
            if joined in ("", IX_FOOTNOTE):
                continue
            raise ParseError(f"{annex}: unrecognized single-cell row {joined[:60]!r}")
        texts = [clean(element_text(c), annex) for c in cells]
        if len(cells) == 4:
            # A live four-cell row is a sub-entry: its first cell fuses the
            # lettered marker and the name; the parent's number applies.
            if is_expunged(texts):
                continue
            name_lines = cell_texts(cells[0], annex)
            sub = re.match(r"^\(([a-z]{1,2})\) (.+)$", name_lines[0])
            if sub is None:
                raise ParseError(f"{annex}: four-cell row {texts[0][:40]!r}")
            record_id = f"{last[0]} ({sub.group(1)})"
            ctx = f"{annex} entry {record_id}"
            if (last[0], sub.group(1)) <= last:
                raise ParseError(f"{ctx}: entry number out of order")
            last = (last[0], sub.group(1))
            name_lines[0] = sub.group(2)
            info_td, reason_td, date_td = cells[1], cells[2], cells[3]
        elif len(cells) != 5:
            raise ParseError(f"{annex}: row with {len(cells)} cells")
        elif not texts[0]:
            # A sub-entry may print five cells with an empty number cell
            # and its lettered marker fused into the name cell.
            if is_expunged(texts[1:]):
                continue
            name_lines = cell_texts(cells[1], annex)
            sub = re.match(r"^\(([a-z]{1,2})\) (.+)$", name_lines[0])
            if sub is None:
                raise ParseError(f"{annex}: numberless row {texts[1][:40]!r}")
            record_id = f"{last[0]} ({sub.group(1)})"
            ctx = f"{annex} entry {record_id}"
            if (last[0], sub.group(1)) <= last:
                raise ParseError(f"{ctx}: entry number out of order")
            last = (last[0], sub.group(1))
            name_lines[0] = sub.group(2)
            info_td, reason_td, date_td = cells[2], cells[3], cells[4]
        else:
            # A deleted-in-place row may keep its printed number while
            # every content cell was expunged to a dash or black block.
            if is_expunged(texts[1:]):
                continue
            ctx = f"{annex} entry {texts[0]}"
            record_id = ix_record_id(ctx, cells[0])
            number = int(record_id.split(" ")[0])
            letter = record_id.partition("(")[2].rstrip(")")
            if (number, letter) <= last:
                raise ParseError(f"{ctx}: entry number out of order")
            last = (number, letter)
            name_lines = cell_texts(cells[1], ctx)
            info_td, reason_td, date_td = cells[2], cells[3], cells[4]
        row = Row(annex, schema, MEASURE, record_id=record_id)
        name_cell = cells[0] if len(cells) == 4 else cells[1]
        images = len(xpath_elements(name_cell, ".//img"))
        parse_ix_name(ctx, annex, record_id, name_lines, row, images)
        parse_ix_info(ctx, annex, record_id, info_td, row, schema != "Person")
        row.reason = " ".join(cell_texts(reason_td, ctx))
        date_lines = cell_texts(date_td, ctx)
        if not date_lines:
            raise ParseError(f"{ctx}: empty date cell")
        date_match = IX_DATE_RE.match(date_lines[0])
        if date_match is None:
            raise ParseError(f"{ctx}: unrecognized date {date_lines[0]!r}")
        # A history parenthetical may wrap across further cell lines; some
        # entries print it without its closing parenthesis or with stray
        # trailing list punctuation (misprints).
        extra = " ".join(date_lines[1:]).replace("( ", "(").replace(" )", ")")
        extra = extra.rstrip(";. ").strip()
        if extra and not extra.endswith(")"):
            extra = f"{extra})"
        if extra and IX_HISTORY_RE.match(extra) is None:
            raise ParseError(f"{ctx}: unrecognized date line {extra!r}")
        row.start_date = verbatim_date(date_match.group(1), ctx, DATE_FORMATS)
        rows.append(row)
    return rows


def parse_ix_section(part: str, centered: Element) -> list[Row]:
    section: str | None = None
    table: Element | None = None
    for child in centered:
        cls = child.get("class") or ""
        if child.tag == "p" and cls == "modref":
            check_marker(" ".join(element_text(child).split()), f"IX.{part}")
            continue
        if child.tag == "p":
            text = clean(element_text(child), f"IX.{part}")
            if not text:
                continue
            match = IX_SECTION_RE.match(text)
            if match is None or section is not None:
                raise ParseError(f"IX.{part}: unexpected sub-heading {text!r}")
            section = match.group(1) or match.group(2)
            continue
        if child.tag == "table":
            if section is None or table is not None:
                raise ParseError(f"IX.{part}: table outside a section")
            table = child
            continue
        raise ParseError(f"IX.{part}: unexpected <{child.tag} class={cls!r}>")
    if section is None or table is None:
        raise ParseError(f"IX.{part}: section without heading or table")
    annex = f"IX.{part.rstrip('.')}.{section}"
    return parse_ix_table(annex, IX_SECTION_SCHEMAS[section], table)


def parse_annex_ix(block: Element) -> list[Row]:
    rows: list[Row] = []
    parts = xpath_elements(block, "./div[contains(@class, 'grid-container')]")
    if len(parts) != 3:
        raise ParseError(f"IX: {len(parts)} part containers, expected 3")
    for part, grid in zip(("I.", "II.", "III."), parts, strict=True):
        col1 = xpath_elements(
            grid, "./div[contains(@class, 'grid-list-column-1')]", expect_exactly=1
        )[0]
        numeral = clean(element_text(col1), "IX")
        if numeral != part:
            raise ParseError(f"IX: part {numeral!r}, expected {part!r}")
        col2 = xpath_elements(
            grid, "./div[contains(@class, 'grid-list-column-2')]", expect_exactly=1
        )[0]
        seen_heading = False
        tables = 0
        for child in col2:
            cls = child.get("class") or ""
            if child.tag == "span":
                if clean(element_text(child), f"IX.{part}"):
                    raise ParseError(f"IX.{part}: unexpected span content")
                continue
            if child.tag == "p" and cls == "norm":
                text = clean(element_text(child), f"IX.{part}")
                if not text.startswith(IX_PART_HEADINGS[part]) or seen_heading:
                    raise ParseError(f"IX.{part}: unexpected heading {text[:60]!r}")
                seen_heading = True
                continue
            if child.tag == "p" and cls in SKIP_P_CLASSES:
                if clean(element_text(child), f"IX.{part}"):
                    raise ParseError(f"IX.{part}: unexpected paragraph content")
                continue
            if child.tag == "div" and "centered" in cls:
                rows.extend(parse_ix_section(part, child))
                tables += 1
                continue
            raise ParseError(f"IX.{part}: unexpected <{child.tag} class={cls!r}>")
        if tables != 2:
            raise ParseError(f"IX.{part}: {tables} tables, expected 2")
    return rows


# --- document ----------------------------------------------------------------


def parse_document(doc: Element) -> list[Row]:
    rows: list[Row] = []
    known = {"VIII", "IX"} | NON_TARGET
    for roman, block in annex_blocks(doc, known):
        if roman in NON_TARGET:
            continue
        parsed = parse_annex_viii(block) if roman == "VIII" else parse_annex_ix(block)
        if not parsed:
            raise ParseError(f"{roman}: no designations parsed")
        rows.extend(parsed)
    return rows


@click.command(help="Parse consolidated Regulation 267/2012 into a CSV candidate.")
@click.argument("celex")
@click.option(
    "--source",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    help="Parse these exact XHTML bytes instead of fetching from CELLAR.",
)
def main(celex: str, source: Path | None) -> None:
    try:
        check_registry(PROGRAM_KEY, [MEASURE], ("Person", "LegalEntity"))
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
