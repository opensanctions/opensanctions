"""Parse consolidated Regulation (EU) 2024/1485 (situation in Russia) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation's annexes:

- Annex I — equipment which might be used for internal repression, not
  designations.
- Annex II — interception equipment, technology and software, not
  designations.
- Annex III — competent-authority websites, not designations.
- Annex IV — the Article 6 fund-freeze list, with parts Α. Natural persons
  and Β. Legal persons, entities and bodies (the part letters are printed
  as Greek capitals), each printed as one five-column table (entry number,
  name, identifying information, statement of reasons, date of listing).
  Travel bans live in Decision (CFSP) 2024/1484, not in this regulation.

There is no separate native-script name column: Cyrillic renderings are
printed as parenthetical lines under the Latin name, some annotated
"(Russian: …)", and become aliases. Dates are transcribed as the source
prints them ("27.5.2024"); the crawler normalizes dates. Relational
"Associated …" lines name other parties, have no CSV column, and are
deliberately not transcribed.

Output: data/consolidated/32024R1485.csv (the EU Journal consolidated CSV
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
    table_body,
    to_record,
    validate_records,
    verbatim_date,
    write_csv,
)
from lxml import html
from zavod.helpers.html import element_text, xpath_elements
from zavod.util import Element

FRAMEWORK_CELEX = "32024R1485"
PROGRAM_KEY = "EU-RUS"
# Annex IV implements the regulation's Article 6 fund freeze; travel bans
# live in Decision (CFSP) 2024/1484.
MEASURE = "Asset freeze"

NON_TARGET = frozenset({"I", "II", "III"})

HEADER = (
    "",
    "Name",
    "Identifying information",
    "Statement of Reasons",
    "Date of listing",
)
# (part heading, part id, schema) in print order. The document prints the
# part letters as Greek capital Alpha and Beta, not Latin A and B.
PARTS = (
    ("Α. Natural persons", "A", "Person"),
    ("Β. Legal persons, entities and bodies", "B", "LegalEntity"),
)
# Entries whose printed identifiers force a more specific schema: IPJSC NTK
# is a joint-stock company and carries a KPP, a Company-only property.
SCHEMA_PINS = {("B", "2"): "Company"}

NUMBER_RE = re.compile(r"^(\d+)\.$")
# Alias lines in the name cell: "a.k.a. Mikhail STEPANOV". The label also
# occurs inline, chaining variants on the first line ("IPJSC NTK a.k.a.
# International Public Joint-Stock Company NTK a.k.a. …").
AKA_RE = re.compile(r"^a\.k\.a\.?:? (.+)$")
INLINE_AKA_SPLIT_RE = re.compile(r" a\.k\.a\.? ")
# Cyrillic renderings print as fully parenthesized lines under the Latin
# name, optionally annotated with the script ("(Russian: Николай …)"); the
# annotation labels the rendering and is not name text.
PAREN_NAME_RE = re.compile(r"^\((?:Russian: )?(.+)\)$")
# The script annotation can also trail the first line ("… MKAO NTH
# (Russian: МКАО НТХ)").
RUSSIAN_TAIL_RE = re.compile(r"^(.+) \(Russian: (.+)\)$")
# A printed former-name label: "VK (previously known as Mail.ru group, VK
# Company Limited)".
PREVIOUS_TAIL_RE = re.compile(r"^(.+) \(previously known as (.+)\)$")

# Entries listing an unlabeled variant rendering as a bare line under the
# name; the printed list structure marks it as an alias.
ALIAS_LINE_PINS = frozenset({("A", "49")})
# Reviewed hand-mappings for name-cell lines the line rules cannot place,
# keyed by (part, entry) and the exact line. If the source line changes,
# the lookup misses and the run breaks for re-review.
NAME_OVERRIDES: dict[tuple[str, str], dict[str, tuple[tuple[str, str], ...]]] = {
    # A bare Cyrillic rendering without parentheses, and an abbreviation
    # annotated with a "(RU)" language tag that is not name text.
    ("B", "1"): {
        "ФЕДЕРАЛЬНАЯ СЛУЖБА ИСПОЛНЕНИЯ НАКАЗАНИЙ": (
            ("alias", "ФЕДЕРАЛЬНАЯ СЛУЖБА ИСПОЛНЕНИЯ НАКАЗАНИЙ"),
        ),
        "(ФСИН) (RU)": (("alias", "ФСИН"),),
    },
}

# Identifying-information labels → CSV column, exactly as printed.
INFO_LABELS = {
    "DOB": "birthDate",
    "Date of birth": "birthDate",
    "POB": "birthPlace",
    "Place of birth (town, country)": "birthPlace",
    "Gender": "gender",
    "Nationality": "nationality",
    "Position": "position",
    "Address": "address",
    "Passport number": "passportNumber",
    "Internal passport number": "idNumber",
    "SNILS": "idNumber",
    "TIN": "taxNumber",
    "TIN (TIN)": "taxNumber",
    "ITN": "taxNumber",
    "INN": "innCode",
    "INN (Russian Tax ID)": "innCode",
    "INN (Russian tax ID)": "innCode",
    "INN(Russian tax ID)": "innCode",
    "KPP": "kppCode",
    "OGRN": "ogrnCode",
    "OGRN (Trade register number)": "ogrnCode",
    "OKPO": "okpoCode",
    "Date of registration": "incorporationDate",
    "Phone number": "phone",
    "Email": "email",
    "Website": "website",
    "Russian website": "website",
    "International website": "website",
}
# Columns whose labelled value legitimately continues onto bare follow-on
# lines in this document (a former role printed under the current
# position). Bare lines after any other label are new structure.
CONTINUABLE_COLUMNS = frozenset({"position"})
# Labels with no CSV column, deliberately not transcribed: relational lines
# naming other parties, and "Date of issue", which qualifies the internal
# passport printed above it. The label line and its bare continuation lines
# are consumed.
DROP_LABELS = frozenset({"Associated entities", "Date of issue"})
# Reviewed hand-mappings for identifying-information lines the label rules
# cannot place, keyed by (part, entry) and the exact line. An empty mapping
# drops the line deliberately.
INFO_OVERRIDES: dict[tuple[str, str], dict[str, tuple[tuple[str, str], ...]]] = {
    # A printed placeholder; missing values are empty cells. (A47's
    # "Unknown, Russian Federation" states a country and is kept verbatim.)
    ("A", "46"): {"Place of birth (town, country): Unknown": ()},
    ("A", "15"): {
        "ID NO. 45 01 525454": (("idNumber", "45 01 525454"),),
        # The label overruns the labelled-line pattern; its value follows on
        # the next line and is mapped there.
        "Taxpayer Personal Identification Number (ИНН):": (),
        "7703204586": (("innCode", "7703204586"),),
    },
    # Bare website and email lines printed under the address.
    ("B", "3"): {
        "Vk.com": (("website", "Vk.com"),),
        "Vk.company": (("website", "Vk.company"),),
    },
    ("B", "4"): {
        "www.complatform.ru": (("website", "www.complatform.ru"),),
        "mail@complatform.ru": (("email", "mail@complatform.ru"),),
    },
    # A colon-less identifier line.
    ("B", "5"): {
        "INN (Russian tax ID) 7841476577": (("innCode", "7841476577"),),
    },
}


# Only the dotted form occurs in this document.
DATE_FORMATS = ("dotted",)


def peel_name_tails(name: str, row: Row) -> str:
    """Move trailing "(Russian: …)" / "(previously known as …)" groups off
    the name."""
    while True:
        russian = RUSSIAN_TAIL_RE.match(name)
        if russian is not None:
            row.add("alias", [russian.group(2)])
            name = russian.group(1)
            continue
        previous = PREVIOUS_TAIL_RE.match(name)
        if previous is not None:
            row.add("previousName", [previous.group(2)])
            name = previous.group(1)
            continue
        return name


def parse_name(ctx: str, part: str, record_id: str, td: Element, row: Row) -> None:
    lines = cell_lines(td, ctx)
    if not lines:
        raise ParseError(f"{ctx}: empty name cell")
    overrides = NAME_OVERRIDES.get((part, record_id), {})
    first = peel_name_tails(lines[0], row)
    pieces = INLINE_AKA_SPLIT_RE.split(first)
    row.add("name", [pieces[0]])
    for piece in pieces[1:]:
        row.add("alias", split_values(piece))
    for line in lines[1:]:
        if line in overrides:
            for column, value in overrides[line]:
                row.add(column, [value])
            continue
        aka = AKA_RE.match(line)
        if aka is not None:
            row.add("alias", split_values(aka.group(1)))
            continue
        paren = PAREN_NAME_RE.match(line)
        if paren is not None:
            row.add("alias", [paren.group(1)])
            continue
        if (part, record_id) in ALIAS_LINE_PINS:
            row.add("alias", [line])
            continue
        raise ParseError(f"{ctx}: unrecognized name line {line[:60]!r}")


def parse_info(ctx: str, part: str, record_id: str, td: Element, row: Row) -> None:
    lines = cell_lines(td, ctx)
    overrides = INFO_OVERRIDES.get((part, record_id), {})
    # A labelled line opens a block; bare lines continue the block for the
    # columns the document has shown continuations for (a former role under
    # the position) or extend dropped relational content.
    block: str | None = None
    dropped = False
    for line in lines:
        if line in overrides:
            for column, value in overrides[line]:
                row.add(column, [value])
            block, dropped = None, False
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
                raise ParseError(f"{ctx}: label {label!r} without value")
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


def parse_row(roman: str, part: str, schema: str, tr: Element) -> Row:
    cells = xpath_elements(tr, "./td|./th")
    ctx = f"{roman}.{part}"
    number = NUMBER_RE.match(cell_line(cells[0], ctx))
    if number is None:
        raise ParseError(f"{ctx}: unrecognized entry number cell")
    record_id = number.group(1)
    ctx = f"{ctx} entry {record_id}"
    schema = SCHEMA_PINS.get((part, record_id), schema)
    row = Row(annex_id(roman, part), schema, MEASURE, record_id=record_id)
    parse_name(ctx, part, record_id, cells[1], row)
    parse_info(ctx, part, record_id, cells[2], row)
    row.reason = " ".join(cell_lines(cells[3], ctx))
    row.start_date = verbatim_date(cell_line(cells[4], ctx), ctx, DATE_FORMATS)
    return row


def parse_annex_iv(roman: str, block: Element) -> list[Row]:
    # The annex prints each part as a Greek-lettered heading followed by one
    # centered five-column table.
    rows: list[Row] = []
    part_index = -1
    part_tables = [0 for _ in PARTS]
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
    if part_tables != [1 for _ in PARTS]:
        raise ParseError(f"{roman}: part table counts {part_tables}, expected one each")
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


@click.command(help="Parse consolidated Regulation 2024/1485 into a CSV candidate.")
@click.argument("celex")
@click.option(
    "--source",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    help="Parse these exact XHTML bytes instead of fetching from CELLAR.",
)
def main(celex: str, source: Path | None) -> None:
    try:
        check_registry(
            PROGRAM_KEY,
            [MEASURE],
            [part[2] for part in PARTS] + list(SCHEMA_PINS.values()),
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
