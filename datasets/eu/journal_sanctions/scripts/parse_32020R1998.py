"""Parse Regulation (EU) 2020/1998 (Global Human Rights) into a reviewed CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation's annexes:

- Annex I — the Article 3 fund-freeze list, with parts A. Natural persons
  and B. Legal persons, entities and bodies. Empty in the original act:
  every designation was added by later amending regulations. Travel bans
  live in Decision (CFSP) 2020/1999, not in this regulation.
- Annex II — competent-authority websites, not designations.

The parser currently reads only the original act 32020R1998, whose Annex I
is an empty skeleton in the as-published OJ markup (`oj-doc-ti` titles,
`eli-container` annex divs), and therefore writes a header-only CSV.
Consolidated versions (02020R1998-…) use the consolidation markup and carry
designations; teach the parser that structure before pinning one.

Output: data/consolidated/32020R1998.csv (the EU Journal consolidated CSV
contract, keyed by the framework act). The expression the snapshot was
extracted from is passed as the CELEX argument and pinned in the dataset
YAML's `consolidation` lookup, updated in the same commit as the CSV.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import get_args

import click
from common import (
    ParseError,
    Row,
    clean,
    load_source,
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

FRAMEWORK_CELEX = "32020R1998"
# Only the original act is taught; a consolidated pin needs new annex code.
EXPRESSION_RE = re.compile(r"^32020R1998$")
PROGRAM_KEY = "EU-HR"
# Annex I implements the regulation's Article 3 fund freeze; travel bans
# live in Decision (CFSP) 2020/1999.
MEASURE = "Asset freeze"

NON_TARGET = frozenset({"II"})

ANNEX_TITLE_RE = re.compile(r"^ANNEX ([A-Z]+)$")
ANNEX_I_TITLE = (
    "List of natural or legal persons, entities or bodies referred to in Article 3"
)
# The empty annex prints its part headings as two-cell layout tables.
ANNEX_I_PARTS = (
    ("A.", "Natural persons"),
    ("B.", "Legal persons, entities and bodies"),
)


def annex_blocks(doc: Element, known: set[str]) -> list[tuple[str, Element]]:
    """Locate the as-published OJ annex containers and check the inventory.

    The original act wraps each annex in <div class="eli-container"
    id="anx_…"> with the annex number in its first oj-doc-ti line — unlike
    the consolidation markup that common.annex_blocks reads.
    """
    blocks: list[tuple[str, Element]] = []
    containers = xpath_elements(
        doc, "//div[@class='eli-container' and starts-with(@id, 'anx_')]"
    )
    for container in containers:
        titles = xpath_elements(container, "./p[@class='oj-doc-ti']")
        if not titles:
            raise ParseError(f"annex container {container.get('id')!r} has no title")
        text = clean(element_text(titles[0]), "annex title")
        match = ANNEX_TITLE_RE.match(text)
        if match is None:
            raise ParseError(f"unrecognized annex title {text!r}")
        blocks.append((match.group(1), container))
    seen = [roman for roman, _ in blocks]
    if len(set(seen)) != len(seen):
        raise ParseError("duplicate annex titles in document")
    unknown = set(seen) - known
    if unknown:
        raise ParseError(f"unknown annexes: {sorted(unknown)}")
    missing = known - set(seen)
    if missing:
        raise ParseError(f"expected annexes missing: {sorted(missing)}")
    return blocks


def parse_annex_i(roman: str, block: Element) -> list[Row]:
    """Verify the original act's empty Annex I skeleton; entries break it.

    The annex holds only its two title lines and the two part-heading layout
    tables. Any other content means designations have appeared and this
    parser must be taught their structure.
    """
    titles: list[str] = []
    headings: list[tuple[str, str]] = []
    for child in block.iterchildren():
        if not isinstance(child.tag, str):
            continue
        cls = child.get("class") or ""
        if child.tag == "p" and cls == "oj-doc-ti":
            titles.append(clean(element_text(child), roman))
            continue
        if child.tag == "table":
            rows = xpath_elements(child, ".//tr")
            if len(rows) != 1:
                raise ParseError(f"{roman}: part-heading table has {len(rows)} rows")
            cells = xpath_elements(rows[0], "./td")
            if len(cells) != 2:
                raise ParseError(f"{roman}: part-heading row has {len(cells)} cells")
            letter = clean(element_text(cells[0]), roman)
            heading = clean(element_text(cells[1]), roman)
            headings.append((letter, heading))
            continue
        raise ParseError(f"{roman}: unexpected <{child.tag} class={cls!r}>")
    if titles != [f"ANNEX {roman}", ANNEX_I_TITLE]:
        raise ParseError(f"{roman}: unexpected title lines {titles}")
    if tuple(headings) != ANNEX_I_PARTS:
        raise ParseError(f"{roman}: part headings {headings} != {list(ANNEX_I_PARTS)}")
    return []


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
    # Annex I is legitimately empty in the original act, so no minimum row
    # count applies; parse_annex_i pins the exact empty skeleton instead.
    rows: list[Row] = []
    for roman, block in annex_blocks(doc, {"I"} | NON_TARGET):
        if roman in NON_TARGET:
            continue
        rows.extend(parse_annex_i(roman, block))
    return rows


@click.command(help="Parse Regulation 2020/1998 into a CSV candidate.")
@click.argument("celex")
@click.option(
    "--source",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    help="Parse these exact XHTML bytes instead of fetching from CELLAR.",
)
def main(celex: str, source: Path | None) -> None:
    try:
        check_registry()
        if EXPRESSION_RE.match(celex) is None:
            raise ParseError(
                f"unsupported expression {celex!r}: only the original act "
                "32020R1998 is taught; consolidated versions need new annex code"
            )
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
