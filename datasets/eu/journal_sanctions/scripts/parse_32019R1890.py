"""Parse consolidated Regulation (EU) 2019/1890 (Türkiye drilling) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation's annexes:

- Annex I — the Article 2 fund-freeze list. Every designation has been
  delisted (the last by Implementing Regulation (EU) 2025/2396), so the
  annex currently prints only the deletion dash "—" and this parser emits
  zero rows. A designation reappearing is a review event: no entry format
  has been taught, so any other annex content breaks the run. Travel bans
  live in Decision (CFSP) 2019/1894, not in this regulation.
- Annex II — competent-authority websites, not designations.

Output: data/consolidated/32019R1890.csv (the EU Journal consolidated CSV
contract, keyed by the framework act). The consolidated version the snapshot
was extracted from is passed as the CELEX argument and pinned in the dataset
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
    annex_blocks,
    clean,
    load_source,
    summary,
    to_record,
    validate_records,
    write_csv,
)
from lxml import html
from zavod.helpers.html import element_text
from zavod.stateful.programs import Measure, get_program_by_key
from zavod.util import Element

FRAMEWORK_CELEX = "32019R1890"
CONSOLIDATED_RE = re.compile(r"^02019R1890-\d{8}$")
PROGRAM_KEY = "EU-TUR"
# Annex I implements the regulation's Article 2 fund freeze; travel bans
# live in Decision (CFSP) 2019/1894.
MEASURE = "Asset freeze"

TARGET_ANNEX = "I"
NON_TARGET = frozenset({"II"})

SUBTITLE = (
    "LIST OF NATURAL AND LEGAL PERSONS, ENTITIES AND BODIES REFERRED TO IN ARTICLE 2"
)
# The emptied list prints one paragraph holding the deletion dash.
EMPTY_MARK = "—"


def check_registry() -> None:
    program = get_program_by_key(PROGRAM_KEY)
    if program is None:
        raise ParseError(f"unknown program key {PROGRAM_KEY!r}")
    if MEASURE not in get_args(Measure):
        raise ParseError(f"invalid measure {MEASURE!r}")
    if MEASURE not in program.measures:
        raise ParseError(f"measure {MEASURE!r} not in {PROGRAM_KEY}")


def parse_annex_i(annex: str, block: Element) -> list[Row]:
    rows: list[Row] = []
    seen_subtitle = False
    seen_empty_mark = False
    for child in block.iterchildren():
        if not isinstance(child.tag, str):
            continue
        cls = child.get("class") or ""
        if child.tag == "hr" and cls == "separator-annex":
            continue
        if child.tag == "p" and cls == "":
            if clean(element_text(child), annex) != "":
                raise ParseError(f"{annex}: unexpected paragraph content")
            continue
        if child.tag == "p" and cls == "title-annex-1":
            continue
        if child.tag == "p" and cls == "title-annex-2":
            if clean(element_text(child), annex) != SUBTITLE or seen_subtitle:
                raise ParseError(f"{annex}: unexpected annex subtitle")
            seen_subtitle = True
            continue
        if child.tag == "p" and cls == "norm":
            text = clean(element_text(child), annex)
            if text != EMPTY_MARK or seen_empty_mark:
                # Every designation has been delisted; no entry format has
                # been taught. Any other content is a new designation to
                # review.
                raise ParseError(f"{annex}: designation content present, none taught")
            seen_empty_mark = True
            continue
        raise ParseError(f"{annex}: unexpected <{child.tag} class={cls!r}>")
    if not seen_subtitle:
        raise ParseError(f"{annex}: missing annex subtitle")
    if not seen_empty_mark:
        raise ParseError(f"{annex}: missing emptied-list mark")
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


@click.command(help="Parse consolidated Regulation 2019/1890 into a CSV candidate.")
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
            raise ParseError(f"not a consolidated 2019/1890 CELEX: {celex!r}")
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
