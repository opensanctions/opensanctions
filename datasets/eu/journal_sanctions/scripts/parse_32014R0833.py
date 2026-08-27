"""Parse consolidated Regulation (EU) 833/2014 into a reviewed candidate CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

Output: data/consolidated/32014R0833.csv (the EU Journal consolidated CSV
contract, keyed by the framework act). The consolidated version the snapshot
was extracted from is passed as the CELEX argument and pinned in the dataset
YAML's `config.consolidation`, updated in the same commit as the CSV.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from pathlib import Path

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
    bare_text,
    cell_line,
    cell_lines,
    check_consolidated_celex,
    check_marker,
    check_registry,
    clean,
    load_source,
    single_paragraph,
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

FRAMEWORK_CELEX = "32014R0833"
PROGRAM_KEY = "EU-RUS"

PART_RE = re.compile(r"^Part ([A-Z])(?: – .+)?$")
# The dot is missing on one observed XLII entry (648).
NUMBER_RE = re.compile(r"^(\d+)\.?$")
NUMBERED_NAME_RE = re.compile(r"^(\d+)\.\s+(\S.*)$")
FORMERLY_RE = re.compile(r"^\(formerly (.+)\)$")
# Former vessel names also appear inline: "Kavya (formerly Hana)"; one
# observed entry (XLII 218) never closes the parenthesis.
INLINE_FORMERLY_RE = re.compile(r"^(.+?) \(formerly ([^()]+)\)?$")
# Alias labels also occur embedded mid-value: "Local name: X A.k.a: Y".
EMBEDDED_ALIAS_RE = re.compile(
    r"\s(?:Local names?|Local Name|A\.k\.a\.?|a\.k\.a\.|Aka|Alias(?:es)?"
    r"|Alsiases):\s*"
)

# Annex IV name-cell labels; every observed variant maps to alias.
NAME_ALIAS_LABELS = frozenset(
    {
        "Local name",
        "Local Name",
        "Local names",
        "A.k.a",
        "A.k.a.",
        "a.k.a.",
        "Aka",
        "Alias",
        "Alsiases",
        "Chinese company name",
        "Shenzhen branch",
    }
)
# Annex IV identifying-information labels → FtM property.
ID_LABELS = {
    "Address(es)": "address",
    "Address": "address",
    "Addresses": "address",
    "Registration number": "registrationNumber",
    "Registration Number": "registrationNumber",
    "Website": "website",
    "Websites": "website",
    "Telephone": "phone",
    "Telephone(s)": "phone",
    "Telephones": "phone",
    "Phone": "phone",
    "Email": "email",
    "Emails": "email",
    "E-mail": "email",
    "email": "email",
    "Place of registration": "jurisdiction",
}

ENTITY_TABLE = "Name of the legal person, entity or body"
TARGETS: dict[str, AnnexSpec] = {
    "III": AnnexSpec("grid_list", "LegalEntity", "Financial restrictions"),
    "IV": AnnexSpec(
        "table",
        "LegalEntity",
        "Export control",
        header=("Number", "Name", "Identifying Information", "Date of listing"),
        roles=("recordId", "iv_name", "iv_info", "startDate"),
    ),
    "V": AnnexSpec("plain_list", "LegalEntity", "Financial restrictions"),
    "VI": AnnexSpec("plain_list", "LegalEntity", "Financial restrictions"),
    "XII": AnnexSpec(
        "plain_list",
        "LegalEntity",
        "Financial restrictions",
        list_suffixes=True,
    ),
    "XIII": AnnexSpec(
        "plain_list",
        "LegalEntity",
        "Financial restrictions",
        list_suffixes=True,
    ),
    "XIV": AnnexSpec(
        "table",
        "LegalEntity",
        "Financial restrictions",
        header=(ENTITY_TABLE, "Date of application"),
        roles=("name", "startDate"),
    ),
    "XV": AnnexSpec("plain_list", "LegalEntity", "Services ban"),
    "XIX": AnnexSpec(
        "numbered_list",
        "LegalEntity",
        "Financial restrictions",
        parts=("A", "B", "C"),
    ),
    "XLII": AnnexSpec(
        "table",
        "Vessel",
        "Transportation restrictions",
        header=(
            "",
            "Vessel name",
            "IMO number",
            "Grounds for inclusion",
            "Date of application",
        ),
        roles=("recordId", "vessel_name", "imoNumber", "reason", "startDate"),
    ),
    "XLIV": AnnexSpec(
        "table",
        "LegalEntity",
        "Financial restrictions",
        header=("", ENTITY_TABLE, "Entry into force"),
        roles=("recordId", "name", "startDate"),
    ),
    "XLV": AnnexSpec(
        "table",
        "LegalEntity",
        "Financial restrictions",
        header=(ENTITY_TABLE, "Entry into force"),
        roles=("name", "startDate"),
        parts=("A", "B", "C", "D"),
    ),
    # Part D (refineries, Article 5ae(2a)) carries the same "any transaction"
    # prohibition as the ports, locks and airports of Parts A to C, so the
    # whole annex keeps one measure.
    "XLVII": AnnexSpec(
        "table",
        "Asset",
        "Transportation restrictions",
        header=("", "Name", "Grounds for inclusion", "Date of application"),
        roles=("recordId", "name", "reason", "startDate"),
        parts=("A", "B", "C", "D"),
    ),
    "XLIX": AnnexSpec(
        "table",
        "LegalEntity",
        "Financial restrictions",
        header=(
            "Name of listed legal person, entity or body",
            "Place of registration",
            "Entry into force",
        ),
        roles=("name", "address", "startDate"),
    ),
    "LII": AnnexSpec(
        "table",
        "Asset",
        "Investment ban",
        header=("Number", "Name", "Location"),
        roles=("recordId", "name", "address"),
        parts=("A", "B"),
        country="Russia",
    ),
    "LIII": AnnexSpec(
        "table",
        "Asset",
        "Financial restrictions",
        header=("Crypto-assets or central bank digital currencies", "Entry into force"),
        roles=("name", "startDate"),
    ),
}

EXPECTED_EMPTY = frozenset({"XLIII", "XLVI", "L", "LIV", "LV", "LVI"})
NON_TARGET = frozenset(
    {
        "I",
        "II",
        "VII",
        "VIII",
        "IX",
        "X",
        "XI",
        "XVI",
        "XVII",
        "XVIII",
        "XX",
        "XXI",
        "XXIII",
        "XXIIIH",
        "XXIV",
        "XXV",
        "XXVI",
        "XXVII",
        "XXVIII",
        "XXIX",
        "XXX",
        "XXXI",
        "XXXII",
        "XXXIII",
        "XXXV",
        "XXXVI",
        "XXXVII",
        "XXXVIIIA",
        "XXXVIIIB",
        "XXXIX",
        "XL",
        "XLI",
        "XLVIII",
        "LI",
        # Article 5bc lists third countries whose crypto-asset service
        # providers are off limits; it designates no party.
        "LVII",
    }
)


# Only the dotted and full-month forms occur in this document.
DATE_FORMATS = (
    "dotted",
    "worded",
)


# A table name cell prints one line. Annex XLVII Part D wraps its single
# refinery name after the comma, so the printed lines are pinned to the name
# they spell; a source edit breaks the key and resurfaces the parse error.
WRAPPED_NAME_PINS: dict[tuple[str, ...], str] = {
    ("Kulevi Oil Refinery,", "Georgia"): "Kulevi Oil Refinery, Georgia",
}


def joined_name(td: Element, ctx: str) -> str:
    """The printed name of a table entry, rejoining a pinned line wrap."""
    lines = cell_lines(td, ctx)
    if len(lines) == 1:
        return lines[0]
    pinned = WRAPPED_NAME_PINS.get(tuple(lines))
    if pinned is None:
        raise ParseError(f"{ctx}: unrecognized multi-line name {lines!r}")
    return pinned


def parse_record_id(text: str, ctx: str) -> str:
    match = NUMBER_RE.match(text)
    if match is None:
        raise ParseError(f"{ctx}: unrecognized entry number {text!r}")
    return match.group(1)


def iter_entry_children(
    roman: str, block: Element, parts: tuple[str, ...]
) -> list[tuple[str, Element]]:
    """Walk direct children; bind entry elements to their current part.

    Returns (part, element) pairs for entry-bearing children: div.centered
    table containers, div.list items, grid containers, and bare entry divs.
    """
    entries: list[tuple[str, Element]] = []
    part = ""
    seen_parts: list[str] = []
    for child in block.iterchildren():
        if not isinstance(child.tag, str):
            continue
        cls = child.get("class") or ""
        if child.tag == "hr" and cls == "separator-annex":
            continue
        if child.tag == "p":
            text = clean(element_text(child), roman) if cls != "modref" else ""
            if cls == "modref":
                check_marker(" ".join(element_text(child).split()), roman)
                continue
            if cls in SKIP_P_CLASSES:
                continue
            if cls in ("norm", "title-gr-seq-level-1"):
                match = PART_RE.match(text)
                if match is not None:
                    part = match.group(1)
                    seen_parts.append(part)
                    if not parts:
                        raise ParseError(f"{roman}: unexpected part {text!r}")
                # Non-part norm/heading text is annex prose; carries no rows.
                continue
            raise ParseError(f"{roman}: unexpected <p class={cls!r}> {text[:50]!r}")
        if child.tag == "div":
            entries.append((part, child))
            continue
        raise ParseError(f"{roman}: unexpected element <{child.tag}>")
    if tuple(seen_parts) != parts:
        raise ParseError(f"{roman}: parts {seen_parts} != expected {list(parts)}")
    return entries


# --- parser families ---------------------------------------------------


def parse_plain_list(roman: str, spec: AnnexSpec, block: Element) -> list[Row]:
    rows: list[Row] = []
    for part, div in iter_entry_children(roman, block, spec.parts):
        if div.get("class") != "list":
            raise ParseError(f"{roman}: unexpected div class {div.get('class')!r}")
        name = bare_text(div, roman)
        if spec.list_suffixes:
            name = name.removesuffix("; and").removesuffix(";").strip()
        if not name:
            raise ParseError(f"{roman}: empty list item")
        row = Row(annex_id(roman, part), spec.schema, spec.measure)
        row.add("name", [name])
        rows.append(row)
    return rows


def parse_grid_list(roman: str, spec: AnnexSpec, block: Element) -> list[Row]:
    rows: list[Row] = []
    for part, div in iter_entry_children(roman, block, spec.parts):
        if "grid-container" not in (div.get("class") or ""):
            raise ParseError(f"{roman}: unexpected div class {div.get('class')!r}")
        cells = [c for c in div.iterchildren() if isinstance(c.tag, str)]
        if len(cells) != 2:
            raise ParseError(f"{roman}: grid entry has {len(cells)} columns")
        record_id = parse_record_id(bare_text(cells[0], roman), roman)
        name = single_paragraph(cells[1], roman)
        if not name:
            raise ParseError(f"{roman}: empty name for entry {record_id}")
        row = Row(annex_id(roman, part), spec.schema, spec.measure, record_id=record_id)
        row.add("name", [name])
        rows.append(row)
    return rows


def parse_numbered_list(roman: str, spec: AnnexSpec, block: Element) -> list[Row]:
    rows: list[Row] = []
    for part, div in iter_entry_children(roman, block, spec.parts):
        if not part:
            raise ParseError(f"{roman}: entry before first part heading")
        text = single_paragraph(div, roman)
        match = NUMBERED_NAME_RE.match(text)
        if match is None:
            raise ParseError(f"{roman}: unrecognized entry {text[:60]!r}")
        row = Row(
            annex_id(roman, part), spec.schema, spec.measure, record_id=match.group(1)
        )
        row.add("name", [match.group(2)])
        rows.append(row)
    return rows


def parse_table(roman: str, spec: AnnexSpec, block: Element) -> list[Row]:
    rows: list[Row] = []
    for part, div in iter_entry_children(roman, block, spec.parts):
        if div.get("class") != "centered":
            raise ParseError(f"{roman}: unexpected div class {div.get('class')!r}")
        table = xpath_elements(div, ".//table", expect_exactly=1)[0]
        if spec.parts and not part:
            raise ParseError(f"{roman}: table before first part heading")
        for tr in table_body(roman, table, spec.header):
            rows.append(parse_table_row(roman, part, spec, tr))
    return rows


def parse_table_row(roman: str, part: str, spec: AnnexSpec, tr: Element) -> Row:
    row = Row(annex_id(roman, part), spec.schema, spec.measure)
    cells = xpath_elements(tr, "./td|./th")
    ctx = roman
    for role, td in zip(spec.roles, cells, strict=True):
        if role == "recordId":
            row.record_id = parse_record_id(cell_line(td, ctx), ctx)
            ctx = f"{roman} entry {row.record_id}"
        elif role == "name":
            row.add("name", [joined_name(td, ctx)])
        elif role == "startDate":
            row.start_date = verbatim_date(cell_line(td, ctx), ctx, DATE_FORMATS)
        elif role == "reason":
            # Grounds cells legitimately span paragraphs (XLII, XLVII).
            row.reason = " ".join(cell_lines(td, ctx))
        elif role == "address":
            row.add("address", [cell_line(td, ctx)])
        elif role == "imoNumber":
            imo = cell_line(td, ctx)
            if re.match(r"^\d{7}$", imo) is None:
                raise ParseError(f"{ctx}: unrecognized IMO number {imo!r}")
            row.add("imoNumber", [imo])
        elif role == "vessel_name":
            parse_vessel_name(ctx, cell_lines(td, ctx), row)
        elif role == "iv_name":
            parse_iv_name(ctx, cell_lines(td, ctx), row)
        elif role == "iv_info":
            parse_iv_info(ctx, cell_lines(td, ctx), row)
        else:
            raise ParseError(f"{roman}: unknown cell role {role!r}")
    if spec.country:
        row.add("country", [spec.country])
    return row


def parse_vessel_name(ctx: str, lines: list[str], row: Row) -> None:
    if not lines:
        raise ParseError(f"{ctx}: empty vessel name cell")
    inline = INLINE_FORMERLY_RE.match(lines[0])
    if inline is not None:
        row.add("name", [inline.group(1)])
        row.add("previousName", split_values(inline.group(2)))
    else:
        row.add("name", [lines[0]])
    for line in lines[1:]:
        match = FORMERLY_RE.match(line)
        if match is None:
            raise ParseError(f"{ctx}: unrecognized name line {line!r}")
        row.add("previousName", split_values(match.group(1)))


# --- Annex IV cell grammars ---------------------------------------------


# Unlabelled continuation lines that neither start with "(" nor carry a
# label, pinned by Annex IV entry number and line prefix. Any other
# unlabelled line raises for review instead of merging silently.
IV_CONTINUATIONS = (
    ("594", "Economic Zone (SEZ);"),
    ("148", "jsc-energiya.com/"),
)
# Annex IV entries whose identifying information opens with a bare,
# unlabelled address line (observed once).
IV_BARE_FIRST_ADDRESS = frozenset({"402"})


def merge_labelled_lines(
    ctx: str,
    record_id: str,
    lines: list[str],
    known: frozenset[str] | dict[str, str],
) -> list[str]:
    """Merge pinned continuation lines into their labelled predecessor.

    A line opens a new field only with a known "Label:" prefix; an unknown
    label raises. A label-less line merges into the previous line only when
    it visibly belongs to it — parenthesized, or pinned in IV_CONTINUATIONS —
    and raises otherwise.
    """
    merged: list[str] = []
    for line in lines:
        match = LABELLED_RE.match(line)
        if match is not None and not line.startswith("("):
            if match.group(1) in known:
                merged.append(line)
                continue
            raise ParseError(f"{ctx}: unknown label {match.group(1)!r}")
        continuation = line.startswith("(") or any(
            entry == record_id and line.startswith(prefix)
            for entry, prefix in IV_CONTINUATIONS
        )
        if not merged or not continuation:
            raise ParseError(f"{ctx}: unlabelled line {line[:60]!r}")
        merged[-1] += " " + line
    return merged


def split_aliases(value: str) -> list[str]:
    parts = EMBEDDED_ALIAS_RE.split(value)
    return [alias for part in parts for alias in split_values(part)]


def parse_iv_name(ctx: str, lines: list[str], row: Row) -> None:
    if not lines:
        raise ParseError(f"{ctx}: empty name cell")
    first = lines[0]
    # Observed once: "<name> Local name: <local>" on the first line.
    if " Local name:" in first:
        name, _, local = first.partition(" Local name:")
        row.add("name", [name.strip()])
        row.add("alias", split_aliases(local))
    else:
        row.add("name", [first])
    for line in merge_labelled_lines(ctx, row.record_id, lines[1:], NAME_ALIAS_LABELS):
        match = LABELLED_RE.match(line)
        if match is None or match.group(1) not in NAME_ALIAS_LABELS:
            raise ParseError(f"{ctx}: unlabelled name line {line[:60]!r}")
        row.add("alias", split_aliases(match.group(2)))


def parse_iv_info(ctx: str, lines: list[str], row: Row) -> None:
    # Observed twice: "Website http://…" missing its colon.
    fixed = [re.sub(r"^Website (https?://)", r"Website: \1", line) for line in lines]
    # Observed once: a first line that is a bare, unlabelled address.
    if fixed and LABELLED_RE.match(fixed[0]) is None:
        if row.record_id not in IV_BARE_FIRST_ADDRESS:
            raise ParseError(f"{ctx}: unlabelled first info line {fixed[0][:60]!r}")
        fixed[0] = "Address(es): " + fixed[0]
    for line in merge_labelled_lines(ctx, row.record_id, fixed, ID_LABELS):
        match = LABELLED_RE.match(line)
        if match is None or match.group(1) not in ID_LABELS:
            raise ParseError(f"{ctx}: unlabelled info line {line[:60]!r}")
        row.add(ID_LABELS[match.group(1)], split_values(match.group(2)))


FAMILIES: dict[str, Callable[[str, AnnexSpec, Element], list[Row]]] = {
    "plain_list": parse_plain_list,
    "grid_list": parse_grid_list,
    "numbered_list": parse_numbered_list,
    "table": parse_table,
}


# --- assembly and CLI ------------------------------------------------------


def parse_document(doc: Element) -> list[Row]:
    rows: list[Row] = []
    known = set(TARGETS) | set(EXPECTED_EMPTY) | set(NON_TARGET)
    for roman, block in annex_blocks(doc, known):
        if roman in NON_TARGET:
            continue
        if roman in EXPECTED_EMPTY:
            assert_empty(roman, block)
            continue
        spec = TARGETS[roman]
        annex_rows = FAMILIES[spec.family](roman, spec, block)
        if not annex_rows:
            raise ParseError(f"{roman}: no entries extracted")
        rows.extend(annex_rows)
    return rows


def check_families() -> None:
    """Every target annex must name a reader this parser implements."""
    for roman, spec in TARGETS.items():
        if spec.family not in FAMILIES:
            raise ParseError(f"{roman}: unknown family {spec.family!r}")


@click.command(help="Parse consolidated Regulation 833/2014 into a CSV candidate.")
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
            [spec.measure for spec in TARGETS.values()],
            [spec.schema for spec in TARGETS.values()],
        )
        check_families()
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
