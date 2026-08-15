"""Parse consolidated Regulation (EU) 401/2013 (Myanmar/Burma) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation's annexes:

- Annex I — equipment which might be used for internal repression, not
  designations.
- Annex II — competent-authority websites, not designations.
- Annex III — interception equipment, technology and software, not
  designations.
- Annex IV — the Article 4a fund-freeze list, with parts A. Natural persons
  and B. Legal persons, entities and bodies, each printed as one centered
  five-column table (entry number, name, identifying information, reasons,
  date of listing). Travel bans live in Decision 2013/184/CFSP, not in this
  regulation.

Alias renderings print as "(a.k.a. …)" parentheticals, trailing the name or
on their own line; other parentheses are part of the printed name and stay
in it. A few entries continue into a second table row whose other cells are
empty and whose reasons cell extends the preceding entry. Delisted entries
leave numbering gaps. Relational "Associated …" lines name other parties,
have no CSV column, and are deliberately not transcribed. Dates are
transcribed as the source prints them ("25.6.2018"); the crawler normalizes
dates.

Output: data/consolidated/32013R0401.csv (the EU Journal consolidated CSV
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
    LABELLED_RE,
    SKIP_P_CLASSES,
    ParseError,
    Row,
    annex_blocks,
    annex_id,
    cell_line,
    cell_lines,
    check_consolidated_celex,
    check_marker,
    check_registry,
    clean,
    load_source,
    split_values,
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

FRAMEWORK_CELEX = "32013R0401"
PROGRAM_KEY = "EU-MMR"
# Annex IV implements the regulation's Article 4a fund freeze; travel bans
# live in Decision 2013/184/CFSP.
MEASURE = "Asset freeze"

NON_TARGET = frozenset({"I", "II", "III"})

HEADER = (
    "",
    "Name",
    "Identifying information",
    "Reasons",
    "Date of listing",
)
# (part heading, part id, schema) in print order.
PARTS = (
    ("A. Natural persons referred to in Article 4a", "A", "Person"),
    (
        "B. Legal persons, entities and bodies referred to in Article 4a",
        "B",
        "LegalEntity",
    ),
)

NUMBER_RE = re.compile(r"^(\d+)\.$")
# Alias renderings print under the printed "a.k.a." label, as a trailing
# parenthetical on the name line ("Khin Maung Yi (a.k.a. Khin Maung Yee;
# a.k.a. U Khin Maung Yi)") or as a parenthesized line of their own. The
# label sometimes prints without its final period ("a.k.a Sitt Taing Aung").
AKA_TAIL_RE = re.compile(r"^(.+?) \(a\.k\.a\.? (.+)\)$")
AKA_LINE_RE = re.compile(r"^\(a\.k\.a\.? (.+)\)$")
# Within one a.k.a. list, each further variant is introduced by its own
# printed label ("IGGC, a.k.a. IGG") or a semicolon; commas without a label
# join words of a single variant and never split.
AKA_SEP_RE = re.compile(r"[;,] *a\.k\.a\.? +|; +")

# Identifying-information labels → CSV column, exactly as printed.
INFO_LABELS = {
    "Date of birth": "birthDate",
    "Place of birth": "birthPlace",
    "Gender": "gender",
    "Nationality": "nationality",
    "Function": "position",
    "Rank": "position",
    "Political rank": "position",
    "Address": "address",
    "Place of registration": "address",
    "Principal place of business": "address",
    "Place of business": "address",
    "Branch office": "address",
    "Type of entity": "legalForm",
    "Date of registration": "incorporationDate",
    "Registration number": "registrationNumber",
    "Passport number": "passportNumber",
    "Passport no.": "passportNumber",
    "Passport No": "passportNumber",
    "National ID": "idNumber",
    "National Identification Number": "idNumber",
    "National Identification number": "idNumber",
    "ID number": "idNumber",
    "NRC Number": "idNumber",
    "Citizenship verification card": "idNumber",
    "Military identification number": "idNumber",
    "Military ID": "idNumber",
    "Phone number": "phone",
    "Phone no": "phone",
    "Email": "email",
    "Email address": "email",
    "Website": "website",
}
# Columns whose labelled value legitimately continues onto bare follow-on
# lines in this document (multi-line addresses under a bare "Address:").
CONTINUABLE_COLUMNS = frozenset({"address"})
# Labels with no CSV column, deliberately not transcribed: relational lines
# naming other parties, and the passport-validity dates qualifying the
# passport printed above them. The label line and its continuation lines
# are consumed.
DROP_LABELS = frozenset(
    {
        "Date of issue",
        "Date of expiry",
        "Associates",
        "Associated individual",
        "Associated individuals",
        "Associated entity",
        "Associated entities",
        "Other associated entities",
    }
)
# Reviewed hand-mappings for identifying-information lines the label rules
# cannot place, keyed by (part, entry) and the exact line. An empty mapping
# drops the line deliberately.
INFO_OVERRIDES: dict[tuple[str, str], dict[str, tuple[tuple[str, str], ...]]] = {
    # The role prints as an unlabeled leading line.
    ("A", "26"): {
        "Member of State Administrative Council;": (
            ("position", "Member of State Administrative Council"),
        ),
    },
    # Descriptive prose about the person, printed without a label.
    ("A", "63"): {
        "U Tayza Kyaw is a member of the Myanmar Armed Forces (Tatmadaw) and "
        "occupies various high-ranking positions, including Commander of the "
        "Northern Command and Commander of the Bureau of Special Operations "
        "No 1 (BSO 1). Since 1 January 2024, he is Commander of the Bureau of "
        "Special Operations No 3 (BSO 3), which is in charge of the "
        "operations of the Western Regional Military Headquarters and the "
        "Southern Regional Military Headquarters.": (
            (
                "notes",
                "U Tayza Kyaw is a member of the Myanmar Armed Forces "
                "(Tatmadaw) and occupies various high-ranking positions, "
                "including Commander of the Northern Command and Commander "
                "of the Bureau of Special Operations No 1 (BSO 1). Since "
                "1 January 2024, he is Commander of the Bureau of Special "
                "Operations No 3 (BSO 3), which is in charge of the "
                "operations of the Western Regional Military Headquarters "
                "and the Southern Regional Military Headquarters.",
            ),
        ),
    },
    # A bare "or" between the entry's two printed alternative addresses; the
    # connector is list structure, not a value.
    ("B", "17"): {"or": ()},
    # The address block wraps mid-phrase and mid-token across four lines,
    # printing two addresses; rejoined by hand.
    ("B", "20"): {
        "Room (201), Building (C), Tet Ka Tho Yeik Mon Housing,": (),
        "New University Ave Rd, Yangon, Myanmar;": (
            (
                "address",
                "Room (201), Building (C), Tet Ka Tho Yeik Mon Housing, "
                "New University Ave Rd, Yangon, Myanmar",
            ),
        ),
        "No 30 B room 701/702 Yadanar Inya Condo Than Lwin": (),
        "Rd, Yangon, Yangon, Myanmar": (
            (
                "address",
                "No 30 B room 701/702 Yadanar Inya Condo Than Lwin "
                "Rd, Yangon, Yangon, Myanmar",
            ),
        ),
    },
}
# One date prints with a stray trailing period ("20.2.2023."); date-only
# trimming of that entry's printed value.
DATE_PERIOD_PINS = frozenset({("B", "17")})


# Only the dotted form occurs in this document.
DATE_FORMATS = ("dotted",)


def split_akas(content: str) -> list[str]:
    return [piece.strip() for piece in AKA_SEP_RE.split(content) if piece.strip()]


def parse_name(ctx: str, td: Element, row: Row) -> None:
    lines = cell_lines(td, ctx)
    if not lines:
        raise ParseError(f"{ctx}: empty name cell")
    first = lines[0]
    tail = AKA_TAIL_RE.match(first)
    if tail is not None:
        row.add("name", [tail.group(1)])
        row.add("alias", split_akas(tail.group(2)))
    else:
        # Parentheses without the a.k.a. label are part of the printed name
        # ("No 1 Mining Enterprise (ME 1)") and stay whole.
        row.add("name", [first])
    for line in lines[1:]:
        aka = AKA_LINE_RE.match(line)
        if aka is None:
            raise ParseError(f"{ctx}: unrecognized name line {line[:60]!r}")
        row.add("alias", split_akas(aka.group(1)))


def parse_info(ctx: str, part: str, record_id: str, td: Element, row: Row) -> None:
    lines = cell_lines(td, ctx)
    overrides = INFO_OVERRIDES.get((part, record_id), {})
    # A labelled line opens a block; bare lines continue the block for the
    # columns the document has shown continuations for (multi-line
    # addresses) or extend dropped relational content.
    block: str | None = None
    dropped = False
    for line in lines:
        if line in overrides:
            for column, value in overrides[line]:
                row.add(column, [value])
            continue
        labelled = LABELLED_RE.match(line)
        label = labelled.group(1) if labelled is not None else None
        if label in DROP_LABELS:
            block, dropped = None, True
            continue
        if label is not None and label in INFO_LABELS:
            assert labelled is not None
            column = INFO_LABELS[label]
            value = labelled.group(2)
            if value == "":
                # Only the address label prints bare, opening a block of
                # follow-on lines.
                if column not in CONTINUABLE_COLUMNS:
                    raise ParseError(f"{ctx}: label {label!r} without value")
            else:
                row.add(column, split_values(value))
            block, dropped = column, False
            continue
        if dropped:
            continue
        if block in CONTINUABLE_COLUMNS:
            assert block is not None
            row.add(block, split_values(line))
            continue
        raise ParseError(f"{ctx}: unrecognized info line {line[:60]!r}")


def continue_row(ctx: str, cells: list[Element], previous: Row | None) -> Row:
    """A row whose other cells are empty extends the previous entry's
    reasons cell."""
    if previous is None:
        raise ParseError(f"{ctx}: continuation row before first entry")
    for index in (1, 2, 4):
        if cell_lines(cells[index], ctx):
            raise ParseError(f"{ctx}: continuation row with cell {index} content")
    reason_lines = cell_lines(cells[3], ctx)
    if not reason_lines:
        raise ParseError(f"{ctx}: continuation row without reasons text")
    previous.reason = " ".join([previous.reason, *reason_lines])
    return previous


def parse_row(
    roman: str, part: str, schema: str, tr: Element, previous: Row | None
) -> Row:
    cells = xpath_elements(tr, "./td|./th")
    ctx = f"{roman}.{part}"
    if not cell_lines(cells[0], ctx):
        return continue_row(f"{ctx} after entry", cells, previous)
    number = NUMBER_RE.match(cell_line(cells[0], ctx))
    if number is None:
        raise ParseError(f"{ctx}: unrecognized entry number cell")
    record_id = number.group(1)
    ctx = f"{ctx} entry {record_id}"
    row = Row(annex_id(roman, part), schema, MEASURE, record_id=record_id)
    parse_name(ctx, cells[1], row)
    parse_info(ctx, part, record_id, cells[2], row)
    row.reason = " ".join(cell_lines(cells[3], ctx))
    date_text = cell_line(cells[4], ctx)
    if (part, record_id) in DATE_PERIOD_PINS:
        if not date_text.endswith("."):
            raise ParseError(f"{ctx}: pinned date lost its trailing period")
        date_text = date_text[:-1]
    row.start_date = verbatim_date(date_text, ctx, DATE_FORMATS)
    return row


def parse_part(roman: str, part: str, schema: str, div: Element) -> list[Row]:
    """One part container: an optional marker, the part heading, one table."""
    ctx = f"{roman}.{part}"
    heading: str | None = None
    tables: list[Element] = []
    for child in div.iterchildren():
        if not isinstance(child.tag, str):
            continue
        cls = child.get("class") or ""
        if child.tag == "p" and cls == "modref":
            check_marker(" ".join(element_text(child).split()), ctx)
            continue
        if child.tag == "p" and cls == "norm":
            if heading is not None:
                raise ParseError(f"{ctx}: multiple part headings")
            heading = clean(element_text(child), ctx)
            continue
        if child.tag == "p" and cls in SKIP_P_CLASSES:
            continue
        if child.tag == "table":
            tables.append(child)
            continue
        raise ParseError(f"{ctx}: unexpected <{child.tag} class={cls!r}>")
    if heading is None or len(tables) != 1:
        raise ParseError(f"{ctx}: expected one heading and one table")
    rows: list[Row] = []
    previous: Row | None = None
    for tr in table_body(ctx, tables[0], HEADER):
        row = parse_row(roman, part, schema, tr, previous)
        if row is not previous:
            rows.append(row)
        previous = row
    return rows


def parse_annex_iv(roman: str, block: Element) -> list[Row]:
    # The annex prints each part as one centered container holding the part
    # heading and its five-column table.
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
        if child.tag == "div" and cls == "centered":
            part_index += 1
            if part_index >= len(PARTS):
                raise ParseError(f"{roman}: more part containers than parts")
            heading, part, schema = PARTS[part_index]
            norms = xpath_elements(child, "./p[@class='norm']", expect_exactly=1)
            if clean(element_text(norms[0]), roman) != heading:
                raise ParseError(f"{roman}: unexpected part heading")
            rows.extend(parse_part(roman, part, schema, child))
            continue
        raise ParseError(f"{roman}: unexpected <{child.tag} class={cls!r}>")
    if part_index != len(PARTS) - 1:
        raise ParseError(
            f"{roman}: found {part_index + 1} parts, expected {len(PARTS)}"
        )
    return rows


def parse_document(doc: Element) -> list[Row]:
    rows: list[Row] = []
    for roman, block in annex_blocks(doc, {"IV"} | NON_TARGET):
        if roman in NON_TARGET:
            continue
        annex_rows = parse_annex_iv(roman, block)
        if not annex_rows:
            raise ParseError(f"{roman}: no entries extracted")
        rows.extend(annex_rows)
    return rows


@click.command(help="Parse consolidated Regulation 401/2013 into a CSV candidate.")
@click.argument("celex")
@click.option(
    "--source",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    help="Parse these exact XHTML bytes instead of fetching from CELLAR.",
)
def main(celex: str, source: Path | None) -> None:
    try:
        check_registry(PROGRAM_KEY, [MEASURE], [part[2] for part in PARTS])
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
