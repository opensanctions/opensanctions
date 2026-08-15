"""Parse consolidated Regulation (EU) 2020/1998 (Global Human Rights) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation's annexes:

- Annex I — the Article 3 fund-freeze list, with parts A. Natural persons
  and B. Legal persons, entities and bodies, each printed as one six-column
  table (entry number, Latin-script name, native-script name, identifying
  information, reasons, date of listing). Travel bans live in Decision
  (CFSP) 2020/1999, not in this regulation.
- Annex II — competent-authority websites, not designations.

A few entries continue into a second table row whose number and name cells
are empty; the continuation's identifying information and reasons text
belong to the preceding entry. Delisted entries leave numbering gaps.
Entries marked "(*1)" are exempt from Article 5(-1); the marker has no CSV
column and is dropped. Relational "Associated …" lines name other parties,
have no CSV column, and are deliberately not transcribed. Dates are
transcribed as the source prints them ("13.12.2021", "2 March 2021"); the
crawler normalizes dates.

Output: data/consolidated/32020R1998.csv (the EU Journal consolidated CSV
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
    to_record,
    validate_records,
    verbatim_date,
    write_csv,
)
from lxml import html
from zavod.helpers.html import element_text, xpath_elements
from zavod.util import Element

FRAMEWORK_CELEX = "32020R1998"
PROGRAM_KEY = "EU-HR"
# Annex I implements the regulation's Article 3 fund freeze; travel bans
# live in Decision (CFSP) 2020/1999.
MEASURE = "Asset freeze"

NON_TARGET = frozenset({"II"})

# Annex I prints each part as a grid-list: the letter in column 1, the part
# heading and one entry table in column 2. The name column headers differ in
# number between the parts.
PART_HEADER_A = (
    "",
    "Names (Transliteration into Latin script)",
    "Names",
    "Identifying information",
    "Reasons for listing",
    "Date of listing",
)
PART_HEADER_B = (
    "",
    "Name (Transliteration into Latin script)",
    "Name",
    "Identifying information",
    "Reasons for listing",
    "Date of listing",
)
# (letter cell, part heading, part id, schema, table header) in print order.
PARTS = (
    ("A.", "Natural persons", "A", "Person", PART_HEADER_A),
    ("B.", "Legal persons, entities and bodies", "B", "LegalEntity", PART_HEADER_B),
)

# The Article 5(-1) exemption footnote printed as a single-cell table row.
FOOTNOTE = "(*1) Article 5(-1) shall not apply to entries identified with an asterisk."
# Entry numbers, optionally carrying the "(*1)" exemption marker ("118 (*1).").
NUMBER_RE = re.compile(r"^(\d+)(?: \(\*\d+\))?\.$")
# The exemption marker also trails some printed names; it is not name text.
ASTERISK_NAME_RE = re.compile(r"^(.*) \(\*\d+\)$")
# Alias lines in the name cell: "a.k.a. X; Y" (one prints "a.k.a Sayf …",
# B22's native cell "a.k.a"). The label also occurs inline after the name
# ("Naji Ibrahim SHARIFI-ZINDASHTI a.k.a. KENANI, Emirhan") and bare, with
# the aliases following as their own lines.
AKA_RE = re.compile(r"^a\.k\.a\.?:? (.+)$")
AKA_BARE_RE = re.compile(r"^a\.k\.a\.?$")
INLINE_AKA_RE = re.compile(r"^(.+?) a\.k\.a\.?:? (.+)$")
# Native-script cells annotate each rendering group with its script and may
# add an alias parenthetical: "Группа Вагнера / (a.k.a ЧВК ‘Вагнер’) /
# (Russian spelling)". The annotation labels the group and is not name text.
SPELLING_RE = re.compile(r"^\([A-Za-z]+ spelling\)$")
INLINE_SPELLING_RE = re.compile(r"^(.+) \([A-Za-z]+ spelling\)$")
PAREN_AKA_RE = re.compile(r"^\(a\.k\.a\.?:? (.+)\)$")
# Alias or former-name parentheticals at the end of a printed name:
# "Wagner Group (a.k.a. Vagner Group, PMC Wagner)", "Kaniyat Militia
# (f.k.a. 7th Brigade) (a.k.a. 9th Brigade)". A parenthetical in the middle
# of a name ("National Security Office (a.k.a. National Security Agency) of
# the Government of Eritrea") stays whole for the crawler's review system.
NAME_PAREN_TAIL_RE = re.compile(r"^(.+) \((a\.k\.a|f\.k\.a)\.?:? (.+)\)$")
# A printed former-name label under the name: "(Maiden name: GARIPOVA)".
MAIDEN_NAME_RE = re.compile(r"^\(Maiden name: (.+)\)$")

# Entries whose name wraps onto a second line (given names / surname as
# separate paragraphs) — the lines join into one name, they are not aliases.
WRAPPED_NAME_PINS = frozenset({("A", "56")})
# Entries listing unlabeled variant names as bare lines under the name; the
# printed list structure marks them as aliases.
ALIAS_LINE_PINS = frozenset({("B", "8")})
# Native-script cells whose annotation group wraps one rendering across
# lines (given names / surname); default is one variant rendering per line.
NATIVE_WRAP_PINS = frozenset(
    {("A", "39"), ("A", "49"), ("A", "52"), ("A", "55"), ("A", "56"), ("A", "62")}
)

# Identifying-information labels → CSV column, exactly as printed.
INFO_LABELS = {
    "DOB": "birthDate",
    "POB": "birthPlace",
    "Gender": "gender",
    "Nationality": "nationality",
    "Position": "position",
    "Position(s)": "position",
    "Function": "position",
    "Rank": "position",
    "Title": "position",
    "Address": "address",
    "Passport": "passportNumber",
    "Passport No.": "passportNumber",
    "Passport no.": "passportNumber",
    "Passport number": "passportNumber",
    "Passport or ID number": "idNumber",
    "ID": "idNumber",
    "ID Number": "idNumber",
    "ID number": "idNumber",
    "Armed forces identification number": "idNumber",
    "Russian armed forces personnel number": "idNumber",
    "Wagner Group ID": "idNumber",
    "Individual Taxpayer Number": "taxNumber",
    "Tax identification number": "taxNumber",
    "Tax ID number": "taxNumber",
    "Tax ID Russia (ИНН)": "innCode",
    "Tax ID Ukraine (ДРФО)": "taxNumber",
    "DRFO code": "taxNumber",
    "Fiscal Registration Number": "taxNumber",
    "Fiscal registration no.": "taxNumber",
    "Registration number": "registrationNumber",
    "Date of registration": "incorporationDate",
    # Mostly cities and street addresses ("106 Guangming Road, Urumqi, …"),
    # only rarely a bare country — a place, not a jurisdiction.
    "Place of registration": "address",
    "Principal place of business": "address",
    "Principal place of activities": "address",
    "Email": "email",
    "Telephone": "phone",
    "Fax": "phone",
    "Website": "website",
    "Telegram": "website",
    "Type of entity": "legalForm",
    "Active regions": "country",
}
# Free-text labels whose prose value goes to `notes`, label stripped.
NOTES_LABELS = frozenset(
    {
        "Other information",
        "Other identifying information",
    }
)
# Columns whose labelled value legitimately continues onto bare follow-on
# lines in this document (positions spanning paragraphs, address lists,
# notes prose). Bare lines after any other label are new structure.
CONTINUABLE_COLUMNS = frozenset({"position", "address", "notes"})
# Labels with no CSV column, deliberately not transcribed: relational lines
# naming other parties, and passport-validity dates qualifying the passport
# printed above them. The label line and its bare continuation lines are
# consumed.
DROP_LABELS = frozenset(
    {
        "Date of delivery",
        "Expiration date",
        "Associated companies",
        "Associated entities",
        "Associated entity",
        "Associated individual",
        "Associated individuals",
        "Associated individual(s)",
        "Associated with",
        "Other associated entities",
    }
)
# Reviewed hand-mappings for identifying-information lines the label rules
# cannot place, keyed by (part, entry) and the exact line. If the source
# line changes, the lookup misses and the run breaks for re-review. An
# empty mapping drops the line deliberately (relational content).
INFO_OVERRIDES: dict[tuple[str, str], dict[str, tuple[tuple[str, str], ...]]] = {
    ("A", "47"): {
        "Other identifying information: Tel.: +74956298447 (office)": (
            ("phone", "+74956298447 (office)"),
        ),
    },
    # The cell names the body's head — relational, no column.
    ("B", "4"): {"Headed by Major General Abraha Kassa": ()},
    # An "Other information" heading that only introduces associates.
    ("B", "7"): {
        "Other information: associated individuals and entities:": (),
        (
            "Yevgeny Prigozhin (deceased), Wagner Group, Dimitri Sytii, "
            "Valery Zakharov, Perfilev, Svetlana Troitskaya, Lobaye Invest"
        ): (),
    },
    # A colon-less identifier line after the contact labels.
    ("B", "20"): {"INN no. 7707314029": (("innCode", "7707314029"),)},
    ("B", "11"): {
        (
            "Other information: Tax ID number: 7811636632; "
            "Government gazette number: 06513574"
        ): (
            ("taxNumber", "7811636632"),
            ("registrationNumber", "06513574"),
        ),
    },
}
# One listing date is printed with a stray trailing period ("28.5.2025.");
# the period is list punctuation, not date wording.
DATE_PERIOD_PINS = frozenset({("B", "35")})
# Implementing Regulation (EU) 2025/1396 numbered its new entry "34." even
# though I.B already listed a 34 (Sultan Sulaiman Shah Brigade). A number
# that duplicates another entry's is no identifier; it is left empty rather
# than invented (the sequence position would be 37).
DUPLICATE_NUMBER_PIN = ("B", "34", "The Zindashti Network")


# Only the dotted and full-month forms occur in this document.
DATE_FORMATS = (
    "dotted",
    "worded",
)


def table_body(roman: str, table: Element, header: tuple[str, ...]) -> list[Element]:
    """Validate the header row and return data rows.

    Skips marker rows and the Article 5(-1) footnote row.
    """
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
            if text == FOOTNOTE:
                continue
            check_marker(text, roman)
            continue
        if len(cells) != len(header):
            raise ParseError(
                f"{roman}: row has {len(cells)} cells, expected {len(header)}"
            )
        body.append(tr)
    return body


def drop_exempt_marker(text: str) -> str:
    # The "(*1)" Article 5(-1) exemption marker is not name text.
    exempt = ASTERISK_NAME_RE.match(text)
    return exempt.group(1) if exempt is not None else text


def split_akas(text: str) -> list[str]:
    # Alias lists split on ";" only; a comma-joined piece stays whole as one
    # value and is categorised in the crawler's review system. A trailing
    # comma is list punctuation continuing onto the next printed line.
    pieces = (piece.strip().rstrip(",") for piece in text.split(";"))
    return [drop_exempt_marker(piece) for piece in pieces if piece]


def peel_name_parentheticals(name: str, row: Row) -> str:
    """Move trailing "(a.k.a. …)" / "(f.k.a. …)" groups off the name."""
    while True:
        tail = NAME_PAREN_TAIL_RE.match(name)
        if tail is None:
            return name
        column = "alias" if tail.group(2) == "a.k.a" else "previousName"
        row.add(column, split_akas(tail.group(3)))
        name = tail.group(1)


def parse_name(ctx: str, part: str, record_id: str, td: Element, row: Row) -> None:
    lines = cell_lines(td, ctx)
    if not lines:
        raise ParseError(f"{ctx}: empty name cell")
    aliases: list[str] = []
    name = peel_name_parentheticals(drop_exempt_marker(lines[0]), row)
    inline = INLINE_AKA_RE.match(name)
    if inline is not None:
        name = inline.group(1)
        aliases.extend(split_akas(inline.group(2)))
    name_parts = [name]
    in_aka_block = inline is not None
    for line in lines[1:]:
        if AKA_BARE_RE.match(line) is not None:
            # A bare "a.k.a." heading; the aliases follow as bare lines.
            in_aka_block = True
            continue
        aka = AKA_RE.match(line) or PAREN_AKA_RE.match(line)
        if aka is not None:
            aliases.extend(split_akas(aka.group(1)))
            in_aka_block = True
            continue
        maiden = MAIDEN_NAME_RE.match(line)
        if maiden is not None:
            row.add("previousName", [maiden.group(1)])
            continue
        if in_aka_block or (part, record_id) in ALIAS_LINE_PINS:
            aliases.extend(split_akas(line))
            continue
        if (part, record_id) in WRAPPED_NAME_PINS:
            name_parts.append(drop_exempt_marker(line))
            continue
        raise ParseError(f"{ctx}: unrecognized name line {line[:60]!r}")
    row.add("name", [" ".join(name_parts)])
    row.add("alias", aliases)


def parse_native_name(
    ctx: str, part: str, record_id: str, td: Element, row: Row
) -> None:
    # The native-script renderings are aliases; the Latin transliteration
    # column holds the primary name. A "(X spelling)" annotation closes a
    # group of lines: one wrapped rendering for pinned entries, otherwise
    # one variant rendering per line.
    group: list[str] = []

    def close_group() -> None:
        if not group:
            return
        if (part, record_id) in NATIVE_WRAP_PINS:
            row.add("alias", [" ".join(group)])
        else:
            row.add("alias", group)
        group.clear()

    for line in cell_lines(td, ctx):
        if line == "-":
            # A printed placeholder for entries without a native rendering.
            continue
        if SPELLING_RE.match(line) is not None:
            close_group()
            continue
        if AKA_BARE_RE.match(line) is not None:
            # A bare "a.k.a." separator between renderings.
            close_group()
            continue
        paren_aka = PAREN_AKA_RE.match(line)
        if paren_aka is not None:
            row.add("alias", split_akas(paren_aka.group(1)))
            continue
        # The script annotation can also trail the rendering on one line.
        inline_annotated = INLINE_SPELLING_RE.match(line)
        if inline_annotated is not None:
            line = inline_annotated.group(1)
        line = peel_name_parentheticals(drop_exempt_marker(line), row)
        if line.startswith("("):
            raise ParseError(f"{ctx}: unrecognized native-name line {line[:60]!r}")
        group.append(line)
        if inline_annotated is not None:
            close_group()
    close_group()


def parse_info(ctx: str, part: str, record_id: str, td: Element, row: Row) -> None:
    lines = cell_lines(td, ctx)
    overrides = INFO_OVERRIDES.get((part, record_id), {})
    # A labelled line opens a block; bare lines continue the block for the
    # columns the document has shown continuations for (positions spanning
    # paragraphs, address lists, notes prose) or extend dropped relational
    # content. `dropped` marks a block with no column.
    block: str | None = None
    opened_empty = False
    dropped = False
    wrapped: str | None = None
    for line in lines:
        if line in overrides:
            for column, value in overrides[line]:
                row.add(column, [value])
            block, opened_empty, dropped, wrapped = None, False, False, None
            continue
        labelled = LABELLED_RE.match(line)
        label = labelled.group(1) if labelled is not None else None
        if label in DROP_LABELS:
            block, opened_empty, dropped, wrapped = None, False, True, None
            continue
        if label in NOTES_LABELS:
            assert labelled is not None
            value = labelled.group(2)
            if value != "":
                row.add("notes", [value])
            block, opened_empty, dropped, wrapped = "notes", False, False, None
            continue
        if label is not None and label in INFO_LABELS:
            assert labelled is not None
            column = INFO_LABELS[label]
            value = labelled.group(2)
            if value != "":
                row.add(column, split_values(value))
            # An empty-valued label ("Passport or ID number:") holds its
            # value on the following bare lines, and a value ending in ";"
            # continues its list there; a value ending in "," wraps
            # mid-phrase onto the next line.
            block, dropped = column, False
            opened_empty = value == "" or value.endswith(";")
            wrapped = column if value.endswith(",") else None
            continue
        if dropped:
            continue
        if wrapped is not None:
            row.props[wrapped][-1] = f"{row.props[wrapped][-1]} {line}"
            wrapped = None
            continue
        if block == "notes":
            row.add("notes", [line])
            continue
        if block is not None and (opened_empty or block in CONTINUABLE_COLUMNS):
            row.add(block, split_values(line))
            continue
        raise ParseError(f"{ctx}: unrecognized info line {line[:60]!r}")


def continue_row(ctx: str, row: Row, cells: list[Element]) -> None:
    """Merge a continuation row (empty number cell) into the previous entry."""
    ctx = f"{ctx} entry {row.record_id} (cont.)"
    for index in (1, 2):
        if cell_lines(cells[index], ctx):
            raise ParseError(f"{ctx}: unexpected name content in continuation row")
    parse_info(ctx, row.annex.rpartition(".")[2], row.record_id, cells[3], row)
    reason_lines = cell_lines(cells[4], ctx)
    if not reason_lines:
        raise ParseError(f"{ctx}: continuation row without reasons text")
    row.reason = " ".join([row.reason, *reason_lines])
    if cell_lines(cells[5], ctx):
        raise ParseError(f"{ctx}: unexpected date in continuation row")


def parse_row(roman: str, part: str, schema: str, tr: Element) -> Row:
    cells = xpath_elements(tr, "./td|./th")
    ctx = f"{roman}.{part}"
    number = NUMBER_RE.match(cell_line(cells[0], ctx))
    if number is None:
        raise ParseError(f"{ctx}: unrecognized entry number cell")
    record_id = number.group(1)
    ctx = f"{ctx} entry {record_id}"
    row = Row(annex_id(roman, part), schema, MEASURE, record_id=record_id)
    parse_name(ctx, part, record_id, cells[1], row)
    parse_native_name(ctx, part, record_id, cells[2], row)
    parse_info(ctx, part, record_id, cells[3], row)
    row.reason = " ".join(cell_lines(cells[4], ctx))
    date_text = cell_line(cells[5], ctx)
    if (part, record_id) in DATE_PERIOD_PINS:
        date_text = date_text.removesuffix(".")
    row.start_date = verbatim_date(date_text, ctx, DATE_FORMATS)
    pinned_part, pinned_number, pinned_name = DUPLICATE_NUMBER_PIN
    if (part, record_id) == (pinned_part, pinned_number):
        if row.props["name"][0].startswith(pinned_name):
            row.record_id = ""
    return row


def parse_part(
    roman: str, grid: Element, spec: tuple[str, str, str, str, tuple[str, ...]]
) -> list[Row]:
    letter, heading, part, schema, header = spec
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
            for tr in table_body(ctx, table, header):
                cells = xpath_elements(tr, "./td|./th")
                if not cell_lines(cells[0], ctx):
                    if not rows:
                        raise ParseError(f"{ctx}: continuation before first entry")
                    continue_row(ctx, rows[-1], cells)
                    continue
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


def parse_document(doc: Element) -> list[Row]:
    rows: list[Row] = []
    for roman, block in annex_blocks(doc, {"I"} | NON_TARGET):
        if roman in NON_TARGET:
            continue
        annex_rows = parse_annex_i(roman, block)
        if not annex_rows:
            raise ParseError(f"{roman}: no entries extracted")
        rows.extend(annex_rows)
    return rows


@click.command(help="Parse consolidated Regulation 2020/1998 into a CSV candidate.")
@click.argument("celex")
@click.option(
    "--source",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    help="Parse these exact XHTML bytes instead of fetching from CELLAR.",
)
def main(celex: str, source: Path | None) -> None:
    try:
        check_registry(
            PROGRAM_KEY, [MEASURE], [schema_name for _, _, _, schema_name, _ in PARTS]
        )
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
