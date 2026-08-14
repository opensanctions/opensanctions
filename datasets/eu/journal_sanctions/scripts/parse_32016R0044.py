"""Parse consolidated Regulation (EU) 2016/44 (Libya) into a reviewed CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation's target annexes:

- Annex II — persons designated by the UN Security Council (Article 6(1)),
  printed in the UN narrative layout: a numbered name heading, one field
  paragraph with a fixed label sequence, and optional narrative paragraphs.
- Annex III — EU-autonomous listings (Article 6(2)) as tables with parts
  A. Persons and B. Entities.
- Annex VI — the Article 5(4) partial freeze (Libyan Investment Authority
  and Libyan Africa Investment Portfolio), in the UN narrative layout with
  entity field labels.

Annex V (vessels) is currently empty; Annexes I, IV and VII list goods and
authorities, not designations. In the narrative layout the trailing
paragraphs are the reason and the field paragraph's "Other information"
goes to notes; entries without narrative paragraphs state no reason.
Dates are transcribed as the source prints them ("26 Feb. 2011",
"Approximately 1952"); the crawler normalizes dates.

Output: data/consolidated/32016R0044.csv (the EU Journal consolidated CSV
contract, keyed by the framework act). The consolidated version the snapshot
was extracted from is passed as the CELEX argument and pinned in the dataset
YAML's `consolidation` lookup, updated in the same commit as the CSV.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from pathlib import Path
from typing import get_args

import click
from common import (
    LABELLED_RE,
    SKIP_P_CLASSES,
    AnnexSpec,
    ParseError,
    Row,
    annex_blocks,
    annex_id,
    assert_empty,
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

FRAMEWORK_CELEX = "32016R0044"
CONSOLIDATED_RE = re.compile(r"^02016R0044-\d{8}$")
PROGRAM_KEY = "EU-LBY"
# Annexes II, III and VI all implement the regulation's fund freezes
# (Articles 6(1), 6(2) and 5(4)); travel bans live in Decision 2015/1333.
MEASURE = "Asset freeze"

NON_TARGET = frozenset({"I", "IV", "VII"})
# Annex V (vessels) was emptied by ▼M19; a repopulation must break the run.
EXPECTED_EMPTY = frozenset({"V"})

# UN narrative headings: "6. Name: 1: ABU 2: ZAYD 3: UMAR 4: DORDA" for
# persons (four name components, unused ones printed as na/n/a), and
# "1. Name: LIBYAN INVESTMENT AUTHORITY" for entities.
PERSON_HEADING_RE = re.compile(r"^(\d+)\. Name: 1: (.+) 2: (.+) 3: (.+) 4: (.+)$")
ENTITY_HEADING_RE = re.compile(r"^(\d+)\. Name: (.+)$")
NOT_AVAILABLE = frozenset({"na", "n/a"})

# The fixed label sequence of a UN narrative field paragraph.
PERSON_LABELS = (
    "Title",
    "Designation",
    "DOB",
    "POB",
    "Good quality a.k.a.",
    "Low quality a.k.a.",
    "Nationality",
    "Passport no",
    "National identification no",
    "Address",
    "Listed on",
    "Other information",
)
ENTITY_LABELS = (
    "A.k.a.",
    "F.k.a.",
    "Address",
    "Listed on",
    "Other information",
)
# Observed casing variants of the field labels (entry 29 prints
# "Passport No:"), mapped back to the canonical label.
LABEL_VARIANTS = {
    "Passport No": "Passport no",
    "National identification No": "National identification no",
}
# UN field label → CSV column for the structured person fields.
PERSON_FIELD_COLUMNS = {
    "Title": "position",
    "Designation": "position",
    "DOB": "birthDate",
    "POB": "birthPlace",
    "Good quality a.k.a.": "alias",
    "Low quality a.k.a.": "weakAlias",
    "Nationality": "nationality",
    "Passport no": "passportNumber",
    "National identification no": "idNumber",
    "Address": "address",
}
ENTITY_FIELD_COLUMNS = {
    "A.k.a.": "alias",
    "F.k.a.": "previousName",
    "Address": "address",
}

ANNEX_III = AnnexSpec(
    "table",
    "Person",
    MEASURE,
    header=("", "Name", "Identifying information", "Reasons", "Date of listing"),
    parts=("A", "B"),
    part_schemas=(("A", "Person"), ("B", "LegalEntity")),
)
III_PART_RE = re.compile(r"^([A-Z])\. (?:Persons|Entities)$")
III_NUMBER_RE = re.compile(r"^(\d+)\.$")
# Inline alias parenthetical in a name cell: "Libyan Agricultural Bank
# (a.k.a. Agricultural Bank; a.k.a. Al Masraf Al Zirae)".
III_INLINE_AKA_RE = re.compile(r"^(.+?) \(a\.k\.a\. (.+)\)$")
# Identifying-information labels → CSV column.
III_INFO_LABELS = {
    "Position": "position",
    "Position (s)": "position",
    "Date of birth": "birthDate",
    "Date of Birth": "birthDate",
    "Place of birth": "birthPlace",
    "Place of Birth": "birthPlace",
    "Passport number": "passportNumber",
    "Gender": "gender",
    "Nationality": "nationality",
    "Callsign": "weakAlias",
    "Wagner Group ID": "idNumber",
    "Believed status/location": "address",
    "Address": "address",
    "Website": "website",
    "E-mail": "email",
    "Tel.": "phone",
}
# Label-less telephone/fax lines in entity cells: "Tel. No. (218)214870586;".
III_PHONE_LINE_RE = re.compile(r"^(?:Tel\. No\.|Fax No\.) (.+?);?$")
# Reviewed hand-splits for identifying-information lines that mix address,
# phone, fax and email in one printed line, keyed by (part, entry) and the
# exact line. If the source line changes, the lookup misses and the run
# breaks for re-review. An empty mapping drops the line deliberately.
III_INFO_OVERRIDES: dict[tuple[str, str], dict[str, tuple[tuple[str, str], ...]]] = {
    ("B", "2"): {
        (
            "Contact details of administration: Hay Alandalus — Jian St. — "
            "Tripoli — PoBox: 1101 — LIBYA Telephone: (+ 218) 214778301 — "
            "Fax (+ 218) 214778766; email: info@gicdf.org"
        ): (
            ("address", "Hay Alandalus — Jian St. — Tripoli — PoBox: 1101 — LIBYA"),
            ("phone", "(+ 218) 214778301"),
            ("phone", "(+ 218) 214778766"),
            ("email", "info@gicdf.org"),
        ),
    },
    ("B", "4"): {
        (
            "Contact details: tel. 00 218 21 444 59 26; 00 21 444 59 00; "
            "fax 00 218 21 340 21 07 http://www.ljbc.net; email: info@ljbc.net"
        ): (
            ("phone", "00 218 21 444 59 26"),
            ("phone", "00 21 444 59 00"),
            ("phone", "00 218 21 340 21 07"),
            ("website", "http://www.ljbc.net"),
            ("email", "info@ljbc.net"),
        ),
    },
    ("B", "6"): {
        (
            "El Ghayran Area, Ganzor El Sharqya, P.O. Box 1100, Tripoli, "
            "Libya; Al Jumhouria Street, East Junzour, Al Gheran, Tripoli, "
            "Libya; Email Address agbank@agribankly.org; SWIFT/BIC AGRULYLT "
            "(Libya);"
        ): (
            (
                "address",
                "El Ghayran Area, Ganzor El Sharqya, P.O. Box 1100, Tripoli, Libya",
            ),
            ("address", "Al Jumhouria Street, East Junzour, Al Gheran, Tripoli, Libya"),
            ("email", "agbank@agribankly.org"),
            ("registrationNumber", "SWIFT/BIC AGRULYLT (Libya)"),
        ),
    },
    ("B", "9"): {
        (
            "Hasan al-Mashay Street (off al- Zawiyah Street) Tel. No.: "
            "(218) 213345187 Fax +218.21.334.5188 email: info@ethic.ly"
        ): (
            ("address", "Hasan al-Mashay Street (off al- Zawiyah Street)"),
            ("phone", "(218) 213345187"),
            ("phone", "+218.21.334.5188"),
            ("email", "info@ethic.ly"),
        ),
    },
    ("B", "17"): {
        # The registrant's personal name has no column in the CSV contract.
        "Registered under name: Kenesbayev Umirbek Zharmenovich": (),
    },
}


def verbatim_date(text: str, ctx: str) -> str:
    # Formats observed in this document: dotted listing dates ("28.2.2011"),
    # UN abbreviated dates ("26 Feb. 2011"), worded dates ("17 March 2011").
    # The printed wording is kept; the recognizers only guard the shape.
    for parse in (parse_dotted_date, parse_worded_date, parse_abbrev_date):
        if parse(text) is not None:
            return text
    raise ParseError(f"{ctx}: unrecognized date {text!r}")


def parse_listed_on(ctx: str, value: str) -> str:
    # "26 Feb. 2011 (amended on 27 Jun. 2014, 1 Apr. 2016)" — the amendment
    # history is not part of the designation date.
    return verbatim_date(value.split(" (amended on ")[0].strip(), ctx)


# --- UN narrative layout (Annexes II and VI) --------------------------------


def narrative_entries(
    roman: str, block: Element, heading_class: str, part_heading: str | None
) -> list[tuple[str, list[str]]]:
    """Collect (heading, following lines) per numbered narrative entry.

    Lines cover field paragraphs, narrative paragraphs, "Additional
    information" sub-headings and grid-list bullets, in document order.
    """
    entries: list[tuple[str, list[str]]] = []
    seen_part = False
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
            # The annex's own title line carries no entries.
            continue
        if child.tag == "p" and cls == heading_class:
            entries.append((text, []))
            continue
        if (
            part_heading is not None
            and child.tag == "p"
            and cls == "title-gr-seq-level-2"
        ):
            if text != part_heading or seen_part:
                raise ParseError(f"{roman}: unexpected part heading {text!r}")
            seen_part = True
            continue
        if child.tag == "p" and cls in ("norm", "title-gr-seq-level-4"):
            if not entries:
                raise ParseError(f"{roman}: text before first entry: {text[:50]!r}")
            entries[-1][1].append(text)
            continue
        if child.tag == "div" and "grid-container" in cls:
            if not entries:
                raise ParseError(f"{roman}: bullet before first entry: {text[:50]!r}")
            entries[-1][1].append(text)
            continue
        raise ParseError(f"{roman}: unexpected <{child.tag} class={cls!r}>")
    if part_heading is not None and not seen_part:
        raise ParseError(f"{roman}: missing part heading {part_heading!r}")
    return entries


def split_labelled_blob(ctx: str, text: str, labels: tuple[str, ...]) -> dict[str, str]:
    """Split a narrative field paragraph at its fixed, ordered labels.

    Labels only count at parenthesis depth zero — alias parentheticals embed
    their own "DOB:"/passport labels — and every label must occur exactly
    once, in order.
    """
    spellings: dict[str, str] = {label: label for label in labels}
    for variant, canonical in LABEL_VARIANTS.items():
        if canonical in labels:
            spellings[variant] = canonical
    found: list[tuple[int, int, str]] = []  # (position, spelling length, label)
    depth = 0
    i = 0
    while i < len(text):
        char = text[i]
        if char == "(":
            depth += 1
        elif char == ")":
            depth = max(0, depth - 1)
        elif depth == 0 and (i == 0 or text[i - 1] == " "):
            for spelling, canonical in spellings.items():
                if text.startswith(spelling + ":", i):
                    found.append((i, len(spelling), canonical))
                    i += len(spelling)
                    break
        i += 1
    sequence = tuple(canonical for _, _, canonical in found)
    if sequence != labels:
        raise ParseError(f"{ctx}: field labels {sequence} != expected {labels}")
    values: dict[str, str] = {}
    for index, (position, length, canonical) in enumerate(found):
        start = position + length + 1
        end = found[index + 1][0] if index + 1 < len(found) else len(text)
        values[canonical] = text[start:end].strip()
    return values


def split_lettered(value: str) -> list[str]:
    """Split a UN multi-value "a) … b) …" list at depth-zero letter markers."""
    if not value.startswith("a) "):
        return [value]
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


def field_values(value: str) -> list[str]:
    """Expand one narrative field into CSV values; na/n/a mean absent."""
    if value in NOT_AVAILABLE:
        return []
    return [item for item in split_lettered(value) if item not in NOT_AVAILABLE]


def assemble_reason(lines: list[str]) -> str:
    """Join the entry's trailing narrative paragraphs."""
    parts: list[str] = []
    for line in lines:
        if line in ("Additional information", "Additional information:"):
            continue
        parts.append(re.sub(r"^—\s*", "", line))
    return " ".join(parts)


def other_information_notes(value: str) -> list[str]:
    """The blob's Other information field as one notes value."""
    if value in NOT_AVAILABLE or value == "":
        return []
    return [value]


def parse_annex_ii(roman: str, block: Element) -> list[Row]:
    rows: list[Row] = []
    entries = narrative_entries(roman, block, "title-gr-seq-level-3", "A. Persons")
    for heading, lines in entries:
        match = PERSON_HEADING_RE.match(heading)
        if match is None:
            raise ParseError(f"{roman}: unrecognized entry heading {heading[:60]!r}")
        record_id = match.group(1)
        ctx = f"{roman} entry {record_id}"
        components = [part for part in match.groups()[1:] if part not in NOT_AVAILABLE]
        if not components:
            raise ParseError(f"{ctx}: name has no components")
        if not lines:
            raise ParseError(f"{ctx}: entry has no field paragraph")
        row = Row(annex_id(roman, "A"), "Person", MEASURE, record_id=record_id)
        row.add("name", [" ".join(components)])
        fields = split_labelled_blob(ctx, lines[0], PERSON_LABELS)
        for label, column in PERSON_FIELD_COLUMNS.items():
            row.add(column, field_values(fields[label]))
        row.start_date = parse_listed_on(ctx, fields["Listed on"])
        row.add("notes", other_information_notes(fields["Other information"]))
        row.reason = assemble_reason(lines[1:])
        rows.append(row)
    return rows


def parse_annex_vi(roman: str, block: Element) -> list[Row]:
    rows: list[Row] = []
    entries = narrative_entries(roman, block, "title-gr-seq-level-2", None)
    for heading, lines in entries:
        match = ENTITY_HEADING_RE.match(heading)
        if match is None:
            raise ParseError(f"{roman}: unrecognized entry heading {heading[:60]!r}")
        record_id = match.group(1)
        ctx = f"{roman} entry {record_id}"
        if not lines:
            raise ParseError(f"{ctx}: entry has no field paragraph")
        row = Row(roman, "LegalEntity", MEASURE, record_id=record_id)
        row.add("name", [match.group(2)])
        fields = split_labelled_blob(ctx, lines[0], ENTITY_LABELS)
        for label, column in ENTITY_FIELD_COLUMNS.items():
            row.add(column, field_values(fields[label]))
        row.start_date = parse_listed_on(ctx, fields["Listed on"])
        row.add("notes", other_information_notes(fields["Other information"]))
        row.reason = assemble_reason(lines[1:])
        rows.append(row)
    return rows


# --- Annex III tables --------------------------------------------------------


def split_iii_aliases(text: str) -> list[str]:
    aliases: list[str] = []
    for piece in text.split(";"):
        piece = piece.strip().removeprefix("a.k.a.").strip()
        if piece:
            aliases.append(piece)
    return aliases


def parse_iii_name(ctx: str, td: Element, row: Row) -> None:
    lines = cell_lines(td, ctx)
    if not lines:
        raise ParseError(f"{ctx}: empty name cell")
    name = lines[0]
    aliases: list[str] = []
    inline = III_INLINE_AKA_RE.match(name)
    if inline is not None:
        name = inline.group(1)
        aliases.extend(split_iii_aliases(inline.group(2)))
    for line in lines[1:]:
        if line.startswith("a.k.a. "):
            aliases.extend(split_iii_aliases(line))
        elif line.startswith("(") and line.endswith(")"):
            # A native-script rendering printed under the Latin name.
            name = f"{name} {line}"
        else:
            raise ParseError(f"{ctx}: unrecognized name line {line[:60]!r}")
    row.add("name", [name])
    row.add("alias", aliases)


def parse_iii_info(ctx: str, part: str, record_id: str, td: Element, row: Row) -> None:
    lines = cell_lines(td, ctx)
    overrides = III_INFO_OVERRIDES.get((part, record_id), {})
    in_address_block = False
    for index, line in enumerate(lines):
        if line in overrides:
            for column, value in overrides[line]:
                row.add(column, [value])
            in_address_block = False
            continue
        phone = III_PHONE_LINE_RE.match(line)
        if phone is not None:
            row.add("phone", [phone.group(1)])
            in_address_block = False
            continue
        labelled = LABELLED_RE.match(line)
        if labelled is not None and labelled.group(1) in III_INFO_LABELS:
            label, value = labelled.group(1), labelled.group(2)
            in_address_block = False
            if label == "Address" and value == "":
                # "Address:" opens a block of bare address lines.
                in_address_block = True
                continue
            if value == "":
                raise ParseError(f"{ctx}: empty value for label {label!r}")
            row.add(III_INFO_LABELS[label], split_values(value))
            continue
        if labelled is not None and labelled.group(1) == "Other info":
            registration = re.match(r"^Reg no (.+)$", labelled.group(2))
            if registration is None:
                raise ParseError(f"{ctx}: unrecognized other info {line[:60]!r}")
            row.add("registrationNumber", [registration.group(1)])
            continue
        # Entity rows open with a bare address line ("Based in Tripoli.");
        # "Address:" blocks continue with bare lines.
        if in_address_block or (part == "B" and index == 0):
            row.add("address", [line.rstrip(";").strip()])
            continue
        raise ParseError(f"{ctx}: unrecognized info line {line[:60]!r}")


def parse_iii_row(roman: str, part: str, tr: Element) -> Row:
    cells = xpath_elements(tr, "./td|./th")
    ctx = f"{roman}.{part}"
    number = III_NUMBER_RE.match(cell_line(cells[0], ctx))
    if number is None:
        raise ParseError(f"{ctx}: unrecognized entry number cell")
    record_id = number.group(1)
    ctx = f"{ctx} entry {record_id}"
    row = Row(
        annex_id(roman, part),
        ANNEX_III.schema_for(part),
        MEASURE,
        record_id=record_id,
    )
    parse_iii_name(ctx, cells[1], row)
    parse_iii_info(ctx, part, record_id, cells[2], row)
    row.reason = " ".join(cell_lines(cells[3], ctx))
    row.start_date = verbatim_date(cell_line(cells[4], ctx), ctx)
    return row


def parse_annex_iii(roman: str, block: Element) -> list[Row]:
    rows: list[Row] = []
    part = ""
    seen_parts: list[str] = []
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
        if child.tag == "div" and cls == "centered":
            if not part:
                raise ParseError(f"{roman}: table before first part heading")
            table = xpath_elements(child, ".//table", expect_exactly=1)[0]
            for tr in table_body(roman, table, ANNEX_III.header):
                rows.append(parse_iii_row(roman, part, tr))
            continue
        text = clean(element_text(child), roman)
        if child.tag == "p" and cls == "title-gr-seq-level-1":
            continue
        if child.tag == "p" and cls == "title-gr-seq-level-2":
            match = III_PART_RE.match(text)
            if match is None:
                raise ParseError(f"{roman}: unrecognized part heading {text!r}")
            part = match.group(1)
            seen_parts.append(part)
            continue
        raise ParseError(f"{roman}: unexpected <{child.tag} class={cls!r}>")
    if tuple(seen_parts) != ANNEX_III.parts:
        raise ParseError(f"{roman}: parts {seen_parts} != {list(ANNEX_III.parts)}")
    return rows


# --- assembly and CLI ------------------------------------------------------


TARGET_PARSERS: dict[str, Callable[[str, Element], list[Row]]] = {
    "II": parse_annex_ii,
    "III": parse_annex_iii,
    "VI": parse_annex_vi,
}


def check_registry() -> None:
    program = get_program_by_key(PROGRAM_KEY)
    if program is None:
        raise ParseError(f"unknown program key {PROGRAM_KEY!r}")
    if MEASURE not in get_args(Measure):
        raise ParseError(f"invalid measure {MEASURE!r}")
    if MEASURE not in program.measures:
        raise ParseError(f"measure {MEASURE!r} not in {PROGRAM_KEY}")
    for schema_name in ("Person", "LegalEntity"):
        if model.get(schema_name) is None:
            raise ParseError(f"unknown schema {schema_name!r}")


def parse_document(doc: Element) -> list[Row]:
    rows: list[Row] = []
    known = set(TARGET_PARSERS) | set(EXPECTED_EMPTY) | set(NON_TARGET)
    for roman, block in annex_blocks(doc, known):
        if roman in NON_TARGET:
            continue
        if roman in EXPECTED_EMPTY:
            assert_empty(roman, block)
            continue
        annex_rows = TARGET_PARSERS[roman](roman, block)
        if not annex_rows:
            raise ParseError(f"{roman}: no entries extracted")
        rows.extend(annex_rows)
    return rows


@click.command(help="Parse consolidated Regulation 2016/44 into a CSV candidate.")
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
            raise ParseError(f"not a consolidated 2016/44 CELEX: {celex!r}")
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
