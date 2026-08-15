"""Parse consolidated Regulation (EU) 2015/1755 (Burundi) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation's annexes:

- Annex I — the Article 2 fund-freeze list, printed as one four-column
  table (entry number, name, identifying information, grounds for
  designation — no date column). Every designation has been delisted
  (the last by Implementing Regulation (EU) 2025/1940), so the table
  currently holds only the header and deletion markers and this parser
  emits zero rows. A designation row reappearing is a review event: no
  entry format has been taught, so any data row breaks the run. Travel
  bans live in Decision (CFSP) 2015/1763, not in this regulation.
- Annex II — competent-authority websites, not designations.

Output: data/consolidated/32015R1755.csv (the EU Journal consolidated CSV
contract, keyed by the framework act). The consolidated version the snapshot
was extracted from is passed as the CELEX argument and pinned in the dataset
YAML's `config.consolidation`, updated in the same commit as the CSV.
"""

from __future__ import annotations

import json
from pathlib import Path

import click
from common import (
    SKIP_P_CLASSES,
    ParseError,
    Row,
    annex_blocks,
    check_consolidated_celex,
    check_marker,
    check_registry,
    clean,
    load_source,
    summary,
    table_body,
    to_record,
    validate_records,
    write_csv,
)
from lxml import html
from zavod.helpers.html import element_text, xpath_elements
from zavod.util import Element

FRAMEWORK_CELEX = "32015R1755"
PROGRAM_KEY = "EU-BDI"
# Annex I implements the regulation's Article 2 fund freeze; travel bans
# live in Decision (CFSP) 2015/1763.
MEASURE = "Asset freeze"

TARGET_ANNEX = "I"
NON_TARGET = frozenset({"II"})

SUBTITLE = (
    "List of natural and legal persons, entities and bodies referred to in Article 2"
)
HEADER = ("", "Name", "Identifying information", "Grounds for designation")


def parse_annex_i(annex: str, block: Element) -> list[Row]:
    rows: list[Row] = []
    seen_subtitle = False
    seen_table = False
    for child in block.iterchildren():
        if not isinstance(child.tag, str):
            continue
        cls = child.get("class") or ""
        if child.tag == "hr" and cls == "separator-annex":
            continue
        if child.tag == "p" and cls == "modref":
            check_marker(" ".join(element_text(child).split()), annex)
            continue
        if child.tag == "p" and cls == "title-gr-seq-level-1":
            if clean(element_text(child), annex) != SUBTITLE or seen_subtitle:
                raise ParseError(f"{annex}: unexpected annex subtitle")
            seen_subtitle = True
            continue
        if child.tag == "p" and cls in SKIP_P_CLASSES:
            continue
        if child.tag == "div" and cls == "centered":
            table = xpath_elements(child, ".//table", expect_exactly=1)[0]
            for tr in table_body(annex, table, HEADER):
                # Every designation has been delisted; no entry format has
                # been taught. A data row is a new designation to review.
                raise ParseError(f"{annex}: designation row present, none taught")
            seen_table = True
            continue
        raise ParseError(f"{annex}: unexpected <{child.tag} class={cls!r}>")
    if not seen_subtitle:
        raise ParseError(f"{annex}: missing annex subtitle")
    if not seen_table:
        raise ParseError(f"{annex}: missing designation table")
    return rows


def parse_document(doc: Element) -> list[Row]:
    rows: list[Row] = []
    for annex, block in annex_blocks(doc, {TARGET_ANNEX} | NON_TARGET):
        if annex in NON_TARGET:
            continue
        # The list is currently empty (all designations delisted), so zero
        # rows is the reviewed state of this annex.
        rows.extend(parse_annex_i(annex, block))
    return rows


@click.command(help="Parse consolidated Regulation 2015/1755 into a CSV candidate.")
@click.argument("celex")
@click.option(
    "--source",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    help="Parse these exact XHTML bytes instead of fetching from CELLAR.",
)
def main(celex: str, source: Path | None) -> None:
    try:
        check_registry(PROGRAM_KEY, [MEASURE], [])
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
