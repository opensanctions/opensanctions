"""Parse consolidated Regulation (EU) 356/2010 (Somalia) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation's target annex:

- Annex I — the UNSCR 1844 designation list (Articles 2 and 8), in a UN
  paragraph layout with sections "I. Persons" (numbered entries; delisted
  entries leave numbering gaps) and "II. Entities" (one unnumbered entry,
  Al-Shabaab; the list prints no entity numbers, so recordId stays empty).
  Each entry is a name heading with an alias parenthetical, labelled field
  lines — older entries run several labelled facts together in one
  paragraph, newer ones print one label per line — and unlabelled narrative
  paragraphs, which are the reason for listing. INTERPOL special-notice
  lines go to notes; "Listed pursuant to paragraph …" clauses state the
  listing basis and join the reason. Travel bans live in Decision
  2010/231/CFSP.

Annex II lists competent-authority websites, not designations. The sibling
Somalia regulation 147/2003 carries only arms-embargo goods lists and no
designations, so this is the regime's sole designation framework. Dates are
transcribed as the source prints them ("12 April 2010", "08 March 2018");
the crawler normalizes dates.

Output: data/consolidated/32010R0356.csv (the EU Journal consolidated CSV
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
    SKIP_P_CLASSES,
    ParseError,
    Row,
    annex_blocks,
    annex_id,
    check_consolidated_celex,
    check_marker,
    check_registry,
    clean,
    load_source,
    summary,
    to_record,
    validate_records,
    verbatim_date,
    write_csv,
)
from lxml import html
from zavod.helpers.html import element_text
from zavod.util import Element

FRAMEWORK_CELEX = "32010R0356"
PROGRAM_KEY = "EU-SOM"
# Annex I implements the Articles 2/8 fund freeze; travel bans live in
# Decision 2010/231/CFSP.
MEASURE = "Asset freeze"

NON_TARGET = frozenset({"II"})

SUBTITLE = "LIST OF NATURAL AND LEGAL PERSONS, ENTITIES OR BODIES REFERRED TO IN ARTICLES 2 AND 8"
# (printed section heading, section id, schema) in print order. Section II
# lists the armed movement Al-Shabaab; it is emitted as Organization.
PARTS = (
    ("I. Persons", "I", "Person"),
    ("II. Entities", "II", "Organization"),
)

# Person entry headings: "8. Fares Mohammed Mana'a (a.k.a.: a) …)". The
# alias parenthetical's label is printed in five spellings; two headings
# close with a sentence period after the paren.
HEADING_RE = re.compile(r"^(\d+)\. (.+)$")
ALIAS_LABELS = ("a.k.a.:", "a.k.a.", "aka:", "aka", "alias:")

# Labelled field segments → CSV column, with the observed casing variants.
# Run-on paragraphs are sliced at ". "/"; " boundaries before a known label.
FIELD_COLUMNS = {
    "Date of birth": "birthDate",
    "Date of Birth": "birthDate",
    "Alt. date of birth": "birthDate",
    "Alt. dates of birth": "birthDate",
    "Place of birth": "birthPlace",
    "Place of Birth": "birthPlace",
    "Nationality": "nationality",
    "Alt. Nationality": "nationality",
    "Citizen": "nationality",
    "Location": "address",
    "Alt. Location": "address",
    "Address": "address",
    "Designation": "position",
    "Gender": "gender",
    "Passport No.": "passportNumber",
    "ID Card No.": "idNumber",
    "Social Security No": "idNumber",
}
DATE_LABEL = "Date of UN designation"
OTHER_LABEL = "Other information"
# Identity-document attributes qualifying the passport/ID printed beside
# them; the contract has no columns for them (Nicaragua precedent).
DROP_LABELS = frozenset({"Place of issue", "Date of issue"})
ALL_LABELS = (
    frozenset(FIELD_COLUMNS) | DROP_LABELS | frozenset({DATE_LABEL, OTHER_LABEL})
)
LABEL_BOUNDARY_RE = re.compile(
    r"(?:^|(?<=\. )|(?<=; ))("
    + "|".join(
        sorted((re.escape(label) for label in ALL_LABELS), key=len, reverse=True)
    )
    + r"):(?: |$)"
)
# A bare line opening like a labelled field but with an untaught label.
SUSPICIOUS_LABEL_RE = re.compile(r"^([A-Z][A-Za-z.' ]{1,30}):(?: |$)")

INTERPOL_PREFIX = "INTERPOL-UN Security Council Special Notice web link:"
BASIS_PREFIX = "Listed pursuant to paragraph"
# A narrative line that looks like a fresh entity heading — a new entity
# appended to section II must break for review, not merge into the reason.
ENTITY_HEADING_RE = re.compile(r"^[A-Z][^:]{0,80} \((a\.k\.a\.|aka|alias)")

# Reviewed value-level hand-mappings, keyed by (section, entry, label) and
# the exact printed value. If the source value changes, the lookup misses
# and the run breaks for re-review.
VALUE_OVERRIDES: dict[tuple[str, str, str, str], tuple[tuple[str, str], ...]] = {
    # The nationality value runs on into a descriptive sentence.
    (
        "I",
        "10",
        "Nationality",
        "United States. Also believed to hold Syrian nationality",
    ): (
        ("nationality", "United States"),
        ("notes", "Also believed to hold Syrian nationality"),
    ),
    # ▼M18 glued a stray footnote numeral onto the printed value.
    ("I", "22", "Gender", "male10"): (("gender", "male"),),
}
# Descriptive Other-information prose reviewed per entry; unreviewed prose
# in that position breaks the run.
OTHER_PROSE_PINS: dict[tuple[str, str], frozenset[str]] = {
    (
        "I",
        "10",
    ): frozenset(
        {
            "Married to a Somali woman. Lived in Egypt in 2005 and moved to "
            "Somalia in 2009. INTERPOL-UN Security Council Special Notice web "
            "link: https://www.interpol.int/en/notice/search/un/5774980"
        }
    ),
}
# The entity heading's alias list misses one space after a comma, gluing
# markers p and q; the reviewed repair restores the printed list structure.
HEADING_REPAIRS = {
    ("II", ""): {
        "Al-Mujaahidiin,q) Harakatul": "Al-Mujaahidiin, q) Harakatul",
    },
}


# Only worded dates occur in this document ("12 April 2010", including the
# zero-padded "08 March 2018").
DATE_FORMATS = ("worded",)


def split_lettered(ctx: str, value: str) -> list[str]:
    """Split a UN lettered list at in-sequence "a) …" / "(a) …" markers.

    Items are separated by ", ", "; " or a bare space before the next
    marker; a trailing list comma/semicolon is punctuation, not value.
    """
    paren = value.startswith("(a) ")
    plain = value.startswith("a) ")
    if not paren and not plain:
        return [value]
    starts: list[int] = [0]
    expected = "b"
    i = 1
    while i < len(value) - 3:
        if value[i - 1] == " ":
            if paren and value[i] == "(" and value[i + 1 : i + 4] == f"{expected}) ":
                starts.append(i)
                expected = chr(ord(expected) + 1)
                i += 4
                continue
            if plain and value[i : i + 3] == f"{expected}) ":
                starts.append(i)
                expected = chr(ord(expected) + 1)
                i += 3
                continue
        i += 1
    skip = 4 if paren else 3
    items: list[str] = []
    for index, start in enumerate(starts):
        end = starts[index + 1] if index + 1 < len(starts) else len(value)
        item = value[start + skip : end].strip().rstrip(",;").strip()
        if not item:
            raise ParseError(f"{ctx}: empty lettered item in {value[:60]!r}")
        items.append(item)
    return items


def field_values(ctx: str, value: str) -> list[str]:
    """Expand one field value into CSV values via the lettered-list split."""
    items = split_lettered(ctx, value)
    for item in items:
        if re.search(r"(?:^|[ ,;])\(?[a-z]\) ", item):
            raise ParseError(f"{ctx}: unsplit lettered marker in {item[:60]!r}")
    return items


def unwrap_quotes(value: str) -> str:
    if len(value) > 1 and value[0] == "‘" and value[-1] == "’":
        return value[1:-1]
    return value


ALIAS_OPEN_RE = re.compile(r" \((a\.k\.a\.:|a\.k\.a\.|aka:|aka|alias:) ?")


def parse_name_aliases(ctx: str, body: str, row: Row) -> None:
    """Split "Name (a.k.a. …)" into the name and its alias values."""
    if body.endswith(")."):
        body = body[:-1]
    if not body.endswith(")"):
        raise ParseError(f"{ctx}: heading without alias parenthetical {body[:60]!r}")
    match = ALIAS_OPEN_RE.search(body)
    if match is None:
        raise ParseError(f"{ctx}: no alias label in heading {body[:60]!r}")
    row.add("name", [body[: match.start()].strip()])
    group = body[match.end() : -1].strip()
    if ALIAS_OPEN_RE.search(group) is not None:
        raise ParseError(f"{ctx}: nested alias group in {group[:60]!r}")
    aliases = [unwrap_quotes(item) for item in field_values(ctx, group)]
    row.add("alias", aliases)


def apply_segment(
    ctx: str, section: str, row: Row, parts: list[str], label: str, value: str
) -> None:
    override = VALUE_OVERRIDES.get((section, row.record_id, label, value))
    if override is not None:
        for column, mapped in override:
            row.add(column, [mapped])
        return
    if label == DATE_LABEL:
        if row.start_date:
            raise ParseError(f"{ctx}: second UN designation date")
        row.start_date = verbatim_date(value, ctx, DATE_FORMATS)
        return
    if label in DROP_LABELS:
        return
    if label == OTHER_LABEL:
        if value == "":
            return
        if value.startswith(INTERPOL_PREFIX):
            row.add("notes", [value])
            return
        if value.startswith(BASIS_PREFIX):
            parts.append(value)
            return
        if value in OTHER_PROSE_PINS.get((section, row.record_id), frozenset()):
            row.add("notes", [value])
            return
        raise ParseError(f"{ctx}: unreviewed Other information {value[:60]!r}")
    if label in FIELD_COLUMNS:
        if value == "":
            raise ParseError(f"{ctx}: empty value for label {label!r}")
        row.add(FIELD_COLUMNS[label], field_values(ctx, value))
        return
    raise ParseError(f"{ctx}: unhandled label {label!r}")


def parse_line(ctx: str, section: str, row: Row, parts: list[str], line: str) -> None:
    """Route one printed paragraph: labelled facts, notice links, narrative."""
    matches = list(LABEL_BOUNDARY_RE.finditer(line))
    if matches and matches[0].start() == 0:
        for index, match in enumerate(matches):
            end = matches[index + 1].start() if index + 1 < len(matches) else len(line)
            value = line[match.end() : end].strip()
            # A trailing "." or ";" here is the separator before the next
            # labelled segment (or the sentence stop), not value text.
            if value.endswith(";") or (
                value.endswith(".") and not value.endswith("..")
            ):
                value = value[:-1].rstrip()
            apply_segment(ctx, section, row, parts, match.group(1), value)
        return
    if SUSPICIOUS_LABEL_RE.match(line) is not None:
        raise ParseError(f"{ctx}: unrecognized field line {line[:60]!r}")
    if ENTITY_HEADING_RE.match(line) is not None:
        raise ParseError(f"{ctx}: possible new entry inside narrative {line[:60]!r}")
    if line.startswith(INTERPOL_PREFIX):
        row.add("notes", [line])
        return
    parts.append(line)


def finish_entry(ctx: str, row: Row, parts: list[str]) -> None:
    if not row.start_date:
        raise ParseError(f"{ctx}: entry has no UN designation date")
    if not parts:
        raise ParseError(f"{ctx}: entry has no reason narrative")
    row.reason = " ".join(parts)


def repair_heading(section: str, record_id: str, text: str) -> str:
    for needle, replacement in HEADING_REPAIRS.get((section, record_id), {}).items():
        if needle not in text:
            raise ParseError(f"{section}: heading repair no longer applies")
        text = text.replace(needle, replacement)
    return text


def parse_annex_i(roman: str, block: Element) -> list[Row]:
    rows: list[Row] = []
    part_index = -1
    section, schema = "", ""
    row: Row | None = None
    parts: list[str] = []
    last_number = 0
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
            text = clean(element_text(child), roman)
            if seen_subtitle or text != SUBTITLE:
                raise ParseError(f"{roman}: unexpected subtitle {text!r}")
            seen_subtitle = True
            continue
        if child.tag == "p" and cls in SKIP_P_CLASSES:
            continue
        if child.tag == "p" and cls == "title-gr-seq-level-2":
            text = clean(element_text(child), roman)
            if part_index + 1 >= len(PARTS) or text != PARTS[part_index + 1][0]:
                raise ParseError(f"{roman}: unexpected section heading {text!r}")
            if row is not None:
                finish_entry(f"{roman}.{section}", row, parts)
                row = None
            part_index += 1
            _, section, schema = PARTS[part_index]
            continue
        text = clean(element_text(child), roman)
        ctx = f"{roman}.{section}"
        if child.tag == "div" and cls == "" and section == "I":
            match = HEADING_RE.match(text)
            if match is None:
                raise ParseError(f"{ctx}: unrecognized entry heading {text[:60]!r}")
            if row is not None:
                finish_entry(ctx, row, parts)
            number = int(match.group(1))
            if number <= last_number:
                raise ParseError(f"{ctx}: entry {number} out of order")
            last_number = number
            row = Row(
                annex_id(roman, section), schema, MEASURE, record_id=match.group(1)
            )
            parts = []
            rows.append(row)
            parse_name_aliases(
                f"{ctx} entry {row.record_id}",
                repair_heading(section, row.record_id, match.group(2)),
                row,
            )
            continue
        if child.tag == "p" and cls == "list" and section == "I":
            if row is None:
                raise ParseError(f"{ctx}: field line before first entry")
            parse_line(f"{ctx} entry {row.record_id}", section, row, parts, text)
            continue
        if child.tag == "p" and cls == "norm" and section == "II":
            if row is None:
                # The section's first paragraph is the unnumbered entity
                # heading; the list prints no entry numbers (never invent).
                row = Row(annex_id(roman, section), schema, MEASURE)
                parts = []
                rows.append(row)
                parse_name_aliases(
                    f"{ctx} entity", repair_heading(section, "", text), row
                )
                continue
            parse_line(f"{ctx} entity", section, row, parts, text)
            continue
        if child.tag == "div" and cls == "grid-container grid-list" and section == "II":
            if row is None:
                raise ParseError(f"{ctx}: bullet before entity heading")
            if not text.startswith("—"):
                raise ParseError(f"{ctx}: unexpected bullet {text[:50]!r}")
            parts.append(text.removeprefix("—").strip())
            continue
        raise ParseError(f"{roman}: unexpected <{child.tag} class={cls!r}>")
    if row is not None:
        finish_entry(f"{roman}.{section}", row, parts)
    if part_index + 1 != len(PARTS):
        raise ParseError(
            f"{roman}: saw {part_index + 1} sections, expected {len(PARTS)}"
        )
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


@click.command(help="Parse consolidated Regulation 356/2010 into a CSV candidate.")
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
