"""Parse consolidated Regulation (EU) 2019/796 (cyber-attacks) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation's annexes:

- Annex I — the Article 3 fund-freeze list, with parts A. Natural persons
  and B. Legal persons, entities and bodies, each printed as one
  five-column table (entry number, name, identifying information, reasons,
  date of listing). Travel bans live in Decision (CFSP) 2019/797, not in
  this regulation.
- Annex II — competent-authority websites, not designations.

There is no separate native-script name column: Cyrillic and Chinese
renderings are printed as bare lines at the top of the identifying
information cell, sometimes annotated "(Russian spelling)" or with a
standalone "(Chinese spelling)" line, and become aliases. An empty-valued
"Alias:"/"Aliases:" label holds its values on the following bare lines.
"Issued by:" and "Validity:" qualify the passport printed above them and
are identity-document metadata with no CSV column; relational
"Associated …" lines name other parties. Both are deliberately not
transcribed. Passport values are kept whole as printed ("EC 867868,
issued on 27.11.1998 (Ukraine)"). Dates are transcribed as the source
prints them ("30.7.2020"); the crawler normalizes dates.

Output: data/consolidated/32019R0796.csv (the EU Journal consolidated CSV
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

FRAMEWORK_CELEX = "32019R0796"
PROGRAM_KEY = "EU-CYB"
# Annex I implements the regulation's Article 3 fund freeze; travel bans
# live in Decision (CFSP) 2019/797.
MEASURE = "Asset freeze"

NON_TARGET = frozenset({"II"})

SUBTITLE = (
    "List of natural and legal persons, entities and bodies referred to in Article 3"
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
    ("A. Natural persons", "A", "Person"),
    ("B. Legal persons, entities and bodies", "B", "LegalEntity"),
)

NUMBER_RE = re.compile(r"^(\d+)\.$")
# An alias line in the name cell: "a.k.a. Maxim Alexandrovich GORDIENKO".
AKA_RE = re.compile(r"^a\.k\.a\.?:? (.+)$")
# Native renderings at the top of the identifying-information cell can
# carry an inline script annotation ("Александр СКЛЯНКО (Russian
# spelling)") or a standalone annotation line ("(Chinese spelling)"); the
# annotation labels the rendering and is not name text.
SPELLING_RE = re.compile(r"^\([A-Za-z]+ spelling\)$")
INLINE_SPELLING_RE = re.compile(r"^(.+) \([A-Za-z]+ spelling\)$")

# Identifying-information labels → CSV column, exactly as printed.
INFO_LABELS = {
    "DOB": "birthDate",
    "Date of birth": "birthDate",
    "POB": "birthPlace",
    "Place of birth": "birthPlace",
    "Gender": "gender",
    "Nationality": "nationality",
    "Address": "address",
    "Location": "address",
    "Place of registration": "address",
    "Principal place of business": "address",
    "Passport number": "passportNumber",
    "Passport": "passportNumber",
    "Alias": "alias",
    "Aliases": "alias",
    "a.k.a.": "alias",
    "Date of registration": "incorporationDate",
    "Registration number": "registrationNumber",
    "Unified Social Credit Code": "registrationNumber",
    "Type of entity": "legalForm",
    "Website": "website",
    "X.com account": "website",
    "Telegram account": "website",
    "Phone numbers": "phone",
    "Email": "email",
}
# Labels whose empty-valued form holds its values on the following bare
# lines (one printed line per alias).
BLOCK_LABELS = frozenset({"Alias", "Aliases"})
# Labels with no CSV column, deliberately not transcribed: relational lines
# naming other parties, and "Issued by"/"Validity", which qualify the
# passport printed above them (identity-document metadata).
DROP_LABELS = frozenset(
    {
        "Associated entity",
        "Associated entities",
        "Associated individual",
        "Associated individuals",
        "Other associated entities",
        "Issued by",
        "Validity",
    }
)
# One address wraps onto a bare follow-on line mid-phrase (no separator);
# the pinned entry joins the line onto the previous address value.
ADDRESS_WRAP_PINS = frozenset({("B", "8")})
# Reviewed hand-mappings for identifying-information lines the label rules
# cannot place, keyed by (part, entry) and the exact line. If the source
# line changes, the lookup misses and the run breaks for re-review.
INFO_OVERRIDES: dict[tuple[str, str], dict[str, tuple[tuple[str, str], ...]]] = {
    # One address whose ";" is intra-address punctuation (building, flat,
    # postal code), not a value separator like I.B 9's two-country list.
    ("A", "20"): {
        (
            "Address: Serebristy Bulvar 34 (Serebristyy Bul’var); krp 1;"
            " flt 528; 197341; St Petersburg, Russian Federation"
        ): (
            (
                "address",
                "Serebristy Bulvar 34 (Serebristyy Bul’var); krp 1; flt 528;"
                " 197341; St Petersburg, Russian Federation",
            ),
        ),
    },
    # The registration-number value embeds its own labelled identifiers.
    ("B", "10"): {
        "Registration number: INN (ИНН): 6168033776; OGRN (ОГРН): 1106194004850": (
            ("innCode", "6168033776"),
            ("ogrnCode", "1106194004850"),
        ),
    },
}


# Only the dotted form occurs in this document.
DATE_FORMATS = ("dotted",)


def parse_name(ctx: str, td: Element, row: Row) -> None:
    lines = cell_lines(td, ctx)
    if not lines:
        raise ParseError(f"{ctx}: empty name cell")
    row.add("name", [lines[0]])
    for line in lines[1:]:
        aka = AKA_RE.match(line)
        if aka is not None:
            row.add("alias", split_values(aka.group(1)))
            continue
        raise ParseError(f"{ctx}: unrecognized name line {line[:60]!r}")


def parse_info(ctx: str, part: str, record_id: str, td: Element, row: Row) -> None:
    lines = cell_lines(td, ctx)
    overrides = INFO_OVERRIDES.get((part, record_id), {})
    # Bare lines before the first label are native-script renderings of the
    # name; after a label they continue an empty-valued alias block, wrap a
    # pinned address, or extend dropped relational content.
    seen_label = False
    block: str | None = None
    dropped = False
    for line in lines:
        if line in overrides:
            for column, value in overrides[line]:
                row.add(column, [value])
            seen_label, block, dropped = True, None, False
            continue
        labelled = LABELLED_RE.match(line)
        label = labelled.group(1) if labelled is not None else None
        if label in DROP_LABELS:
            seen_label, block, dropped = True, None, True
            continue
        if label is not None and label in INFO_LABELS:
            assert labelled is not None
            column = INFO_LABELS[label]
            value = labelled.group(2)
            if value == "":
                if label not in BLOCK_LABELS:
                    raise ParseError(f"{ctx}: label {label!r} without value")
            else:
                row.add(column, split_values(value))
            seen_label, block, dropped = True, column, False
            continue
        if dropped:
            continue
        if not seen_label:
            annotated = INLINE_SPELLING_RE.match(line)
            if annotated is not None:
                row.add("alias", [annotated.group(1)])
                continue
            if SPELLING_RE.match(line) is not None:
                continue
            row.add("alias", [line])
            continue
        if block == "alias":
            row.add("alias", [line])
            continue
        if block == "address" and (part, record_id) in ADDRESS_WRAP_PINS:
            row.props["address"][-1] = f"{row.props['address'][-1]} {line}"
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
    parse_name(ctx, cells[1], row)
    parse_info(ctx, part, record_id, cells[2], row)
    row.reason = " ".join(cell_lines(cells[3], ctx))
    row.start_date = verbatim_date(cell_line(cells[4], ctx), ctx, DATE_FORMATS)
    return row


def parse_annex_i(roman: str, block: Element) -> list[Row]:
    # The annex prints a subtitle, then each part as a lettered heading
    # followed by one centered five-column table.
    rows: list[Row] = []
    part_index = -1
    part_tables = [0 for _ in PARTS]
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
        if child.tag == "p" and cls == "title-gr-seq-level-1":
            heading = clean(element_text(child), roman)
            if heading != SUBTITLE or seen_subtitle:
                raise ParseError(f"{roman}: unexpected subtitle {heading!r}")
            seen_subtitle = True
            continue
        if child.tag == "p" and cls in SKIP_P_CLASSES:
            continue
        if child.tag == "p" and cls == "title-gr-seq-level-2":
            part_index += 1
            if part_index >= len(PARTS):
                raise ParseError(f"{roman}: more part headings than parts")
            heading = clean(element_text(child), roman)
            if heading != PARTS[part_index][0]:
                raise ParseError(f"{roman}: unexpected part heading {heading!r}")
            continue
        if child.tag == "div" and cls == "centered":
            if part_index < 0:
                raise ParseError(f"{roman}: table before first part heading")
            _, part, schema = PARTS[part_index]
            part_tables[part_index] += 1
            table = xpath_elements(child, ".//table", expect_exactly=1)[0]
            for tr in table_body(f"{roman}.{part}", table, HEADER):
                rows.append(parse_row(roman, part, schema, tr))
            continue
        raise ParseError(f"{roman}: unexpected <{child.tag} class={cls!r}>")
    if not seen_subtitle:
        raise ParseError(f"{roman}: annex subtitle missing")
    if part_tables != [1 for _ in PARTS]:
        raise ParseError(f"{roman}: part table counts {part_tables}, expected one each")
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


@click.command(help="Parse consolidated Regulation 2019/796 into a CSV candidate.")
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
