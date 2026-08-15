"""Parse consolidated Regulation (EU) 359/2011 (Iran human rights) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation's annexes:

- Annex I — the Article 2(1) fund-freeze list, printed as two centered
  tables headed "Persons" and "Entities", each five columns (entry number,
  name, identifying information, reasons, date of listing). Travel bans
  live in Decision 2011/235/CFSP, not in this regulation.
- Annex II — competent-authority websites, not designations.
- Annex III — internal-repression equipment, not designations.
- Annex IV — interception equipment, technology and software, not
  designations.

There is no separate native-script name column: Persian renderings print
as extra lines under the Latin name and become aliases, alongside printed
a.k.a. lines and parentheticals (the document spells the label a.k.a.,
aka:, Aka: and a.ka.). A few name cells render the Persian name as an
image captioned "Text of image" and repeat it as text on the next line;
the caption line is pinned and dropped. Some entries continue into a
second table row whose number and name cells are empty; the continuation's
identifying information and reasons belong to the preceding entry, and
fully empty rows are deleted-entry residue. Delisted entries leave
numbering gaps. Relational "Associated …" lines name other parties, have
no CSV column, and are deliberately not transcribed. Dates are transcribed
as the source prints them ("12.4.2011"); the crawler normalizes dates.

Output: data/consolidated/32011R0359.csv (the EU Journal consolidated CSV
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

FRAMEWORK_CELEX = "32011R0359"
PROGRAM_KEY = "EU-IRN"
# Annex I implements the regulation's Article 2(1) fund freeze; travel bans
# live in Decision 2011/235/CFSP.
MEASURE = "Asset freeze"

NON_TARGET = frozenset({"II", "III", "IV"})

SUBTITLE = (
    "List of natural and legal persons, entities and bodies referred to in Article 2(1)"
)
HEADER = (
    "",
    "Name",
    "Identifying information",
    "Reasons",
    "Date of listing",
)
# (table heading, part id, schema) in print order.
PARTS = (
    ("Persons", "A", "Person"),
    ("Entities", "B", "LegalEntity"),
)

NUMBER_RE = re.compile(r"^(\d+)\.$")
# The a.k.a. label as this document prints it: "a.k.a.", "a.k.a", "aka:",
# "Aka:" and the typo "a.ka." — as a line prefix, a fully parenthesized
# line, or a parenthetical trailing the name on its first line.
AKA_LABEL = r"(?:a\.k\.a\.?:?|a\.ka\.|aka:|Aka:)"
AKA_LINE_RE = re.compile(rf"^{AKA_LABEL} (.+)$")
PAREN_AKA_LINE_RE = re.compile(rf"^\({AKA_LABEL} (.+)\)$")
NAME_AKA_TAIL_RE = re.compile(rf"^(.+?) \({AKA_LABEL} (.+)\)$")
# A Persian rendering: Arabic-script text with no Latin letters. Any other
# parenthetical ("(IRGC)", "ARAGHI (ERAGHI) Abdollah") is part of the
# printed name and stays in it for the crawler's name review.
PERSIAN_RE = re.compile(r"^[^A-Za-z]*[؀-ۿ][^A-Za-z]*$")

# Name cells rendering the Persian name as an image captioned "Text of
# image" and repeating it as text on the following line; the caption is
# not name text. The image itself duplicates that text line.
IMAGE_CAPTION = "Text of image"
IMAGE_CAPTION_PINS = frozenset(
    {("A", "185"), ("A", "203"), ("A", "250"), ("A", "251"), ("A", "254")}
)
# One entry prints its a.k.a. parenthetical mid-name ("SEDAQAT (a.k.a.
# Sedaghat) Farajollah"). Extracting it would assemble the name from
# non-contiguous pieces, so the printed name stays whole for the crawler's
# name review; any other mid-name a.k.a. is new structure.
MIDNAME_AKA_PINS = frozenset({("A", "31")})
MIDNAME_AKA_RE = re.compile(rf"\({AKA_LABEL} [^)]+\) \S")

# Identifying-information labels → CSV column, exactly as printed.
INFO_LABELS = {
    "DOB": "birthDate",
    "Date of birth": "birthDate",
    "POB": "birthPlace",
    "Place of birth": "birthPlace",
    "Gender": "gender",
    "Nationality": "nationality",
    "Function": "position",
    "Position": "position",
    "Rank": "position",
    "Military rank": "position",
    "Military Rank": "position",
    "Title": "position",
    "Address": "address",
    "Location": "address",
    "PO Box": "address",
    "Place of residence": "address",
    "Place of work": "address",
    "Place of registration": "address",
    "Principal place of activity": "address",
    "Principal place of business": "address",
    "Type of entity": "legalForm",
    "Date of registration": "incorporationDate",
    "Passport no": "passportNumber",
    "Passport number": "passportNumber",
    "National ID": "idNumber",
    "National ID No": "idNumber",
    "National ID No.": "idNumber",
    "National ID no": "idNumber",
    "National ID number": "idNumber",
    "Iranian national ID no": "idNumber",
    "ID number": "idNumber",
    "Identification No": "idNumber",
    "Birth certificate No": "idNumber",
    "Birth certificate serial No": "idNumber",
    "Registration No": "registrationNumber",
    "Registration number": "registrationNumber",
    "Business registration no": "registrationNumber",
    "Website": "website",
    "Telephone": "phone",
    "Telephone number": "phone",
    "Email": "email",
}
# Columns whose labelled value legitimately continues onto bare follow-on
# lines in this document (a further role under the function, address lists).
# Bare lines after any other label are new structure.
CONTINUABLE_COLUMNS = frozenset({"position", "address"})
# Relational labels naming other parties: no CSV column, deliberately not
# transcribed. The label line and its bare continuation lines are consumed.
DROP_LABELS = frozenset(
    {
        "Associated entities",
        "Associated entity",
        "Associated individuals",
        "Associated individual",
        "Other associated entities",
    }
)


# Only the dotted form occurs in this document.
DATE_FORMATS = ("dotted",)


def parse_name(ctx: str, part: str, record_id: str, td: Element, row: Row) -> None:
    lines = cell_lines(td, ctx)
    if not lines:
        raise ParseError(f"{ctx}: empty name cell")
    first = lines[0]
    if MIDNAME_AKA_RE.search(first) is not None:
        if (part, record_id) not in MIDNAME_AKA_PINS:
            raise ParseError(f"{ctx}: unpinned mid-name a.k.a. in {first[:60]!r}")
    else:
        tail = NAME_AKA_TAIL_RE.match(first)
        if tail is not None:
            first = tail.group(1)
            row.add("alias", split_values(tail.group(2)))
    row.add("name", [first])
    for line in lines[1:]:
        if line == IMAGE_CAPTION:
            if (part, record_id) not in IMAGE_CAPTION_PINS:
                raise ParseError(f"{ctx}: unpinned image caption in name cell")
            continue
        paren_aka = PAREN_AKA_LINE_RE.match(line)
        if paren_aka is not None:
            row.add("alias", split_values(paren_aka.group(1)))
            continue
        aka = AKA_LINE_RE.match(line)
        if aka is not None:
            row.add("alias", split_values(aka.group(1)))
            continue
        if PERSIAN_RE.match(line) is not None:
            row.add("alias", [line])
            continue
        raise ParseError(f"{ctx}: unrecognized name line {line[:60]!r}")


def parse_info(ctx: str, td: Element, row: Row) -> None:
    lines = cell_lines(td, ctx)
    # A labelled line opens a block; bare lines continue the block for the
    # columns the document has shown continuations for (roles spanning
    # lines, address lists) or extend dropped relational content. An
    # empty-valued label ("Principal place of business:") holds its value
    # on the following bare lines, as does a value ending in ";".
    block: str | None = None
    opened_empty = False
    dropped = False
    for line in lines:
        labelled = LABELLED_RE.match(line)
        label = labelled.group(1) if labelled is not None else None
        if label in DROP_LABELS:
            block, opened_empty, dropped = None, False, True
            continue
        if label is not None and label in INFO_LABELS:
            assert labelled is not None
            column = INFO_LABELS[label]
            value = labelled.group(2)
            if value != "":
                row.add(column, split_values(value))
            block, dropped = column, False
            opened_empty = value == "" or value.endswith(";")
            continue
        if dropped:
            continue
        if block is not None and (opened_empty or block in CONTINUABLE_COLUMNS):
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
    row = Row(annex_id(roman, part), schema, MEASURE, record_id=record_id)
    parse_name(ctx, part, record_id, cells[1], row)
    parse_info(ctx, cells[2], row)
    row.reason = " ".join(cell_lines(cells[3], ctx))
    row.start_date = verbatim_date(cell_line(cells[4], ctx), ctx, DATE_FORMATS)
    return row


def continue_row(roman: str, part: str, row: Row, cells: list[Element]) -> None:
    """Merge a continuation row (empty number cell) into the previous entry."""
    ctx = f"{roman}.{part} entry {row.record_id} (cont.)"
    if cell_lines(cells[1], ctx):
        raise ParseError(f"{ctx}: unexpected name content in continuation row")
    if cell_lines(cells[4], ctx):
        raise ParseError(f"{ctx}: unexpected date in continuation row")
    parse_info(ctx, cells[2], row)
    reason_lines = cell_lines(cells[3], ctx)
    if reason_lines:
        row.reason = " ".join([row.reason, *reason_lines]).strip()


def parse_part(roman: str, part: str, schema: str, table: Element) -> list[Row]:
    rows: list[Row] = []
    for tr in table_body(f"{roman}.{part}", table, HEADER):
        cells = xpath_elements(tr, "./td|./th")
        texts = [clean(element_text(td), f"{roman}.{part}") for td in cells]
        if all(text == "" for text in texts):
            # Deleted-entry residue: a fully empty row between markers.
            continue
        if texts[0] == "":
            if not rows:
                raise ParseError(f"{roman}.{part}: continuation before first entry")
            continue_row(roman, part, rows[-1], cells)
            continue
        rows.append(parse_row(roman, part, schema, tr))
    return rows


def parse_annex_i(roman: str, block: Element) -> list[Row]:
    # The annex prints one subtitle, then one centered table per part, each
    # headed by a title-table paragraph ("Persons" / "Entities").
    rows: list[Row] = []
    part_index = -1
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
        if child.tag == "p" and cls == "title-gr-seq-level-1":
            if seen_subtitle:
                raise ParseError(f"{roman}: second annex subtitle")
            if clean(element_text(child), roman) != SUBTITLE:
                raise ParseError(f"{roman}: unexpected annex subtitle")
            seen_subtitle = True
            continue
        if child.tag == "div" and cls == "centered":
            part_index += 1
            if part_index >= len(PARTS):
                raise ParseError(f"{roman}: more part tables than parts")
            heading, part, schema = PARTS[part_index]
            titles = xpath_elements(child, "./p[@class='title-table']")
            if len(titles) != 1 or clean(element_text(titles[0]), roman) != heading:
                raise ParseError(f"{roman}: unexpected part heading in table {part}")
            table = xpath_elements(child, ".//table", expect_exactly=1)[0]
            rows.extend(parse_part(roman, part, schema, table))
            continue
        raise ParseError(f"{roman}: unexpected <{child.tag} class={cls!r}>")
    if part_index != len(PARTS) - 1:
        raise ParseError(f"{roman}: expected {len(PARTS)} part tables")
    if not seen_subtitle:
        raise ParseError(f"{roman}: annex subtitle missing")
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


@click.command(help="Parse consolidated Regulation 359/2011 into a CSV candidate.")
@click.argument("celex")
@click.option(
    "--source",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    help="Parse these exact XHTML bytes instead of fetching from CELLAR.",
)
def main(celex: str, source: Path | None) -> None:
    try:
        check_registry(
            PROGRAM_KEY, [MEASURE], [schema_name for _, _, schema_name in PARTS]
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
