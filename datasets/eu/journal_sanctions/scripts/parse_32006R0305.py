"""Parse consolidated Regulation (EC) 305/2006 (Hariri bombing) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation's annexes:

- Annex I — the Article 2 fund-freeze list, which has never held a
  designation: it prints its subtitle and one bracketed note that the annex
  is to be completed after the committee established by UNSCR 1636 (2005)
  registers persons or entities — which the committee has never done. The
  parser accepts exactly that note and breaks the day anyone is listed, so
  the entry structure gets taught under review. Travel bans live in Common
  Position 2005/888/CFSP.
- Annex II — competent-authority websites, not designations.

The snapshot is therefore a header-only CSV: consolidated files may hold
zero data rows.

Output: data/consolidated/32006R0305.csv (the EU Journal consolidated CSV
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
    annex_blocks,
    check_consolidated_celex,
    check_registry,
    clean,
    load_source,
    summary,
    validate_records,
    write_csv,
)
from lxml import html
from zavod.helpers.html import element_text
from zavod.util import Element

FRAMEWORK_CELEX = "32006R0305"
PROGRAM_KEY = "EU-HARIRI"
# The regulation's Article 2 fund freeze; travel bans live in Common
# Position 2005/888/CFSP.
MEASURE = "Asset freeze"

TARGET_ANNEX = "I"
NON_TARGET = frozenset({"II"})

ANNEX_I_SUBTITLE = (
    "List of natural and legal persons, entities and bodies referred to in Article 2"
)
# The never-used list body: one paragraph holding the printed note that the
# annex awaits registrations by the UNSCR 1636 committee.
PLACEHOLDER = (
    "(Annex to be completed after the persons and entities have been "
    "registered by the Committee established by paragraph 3 (b) of "
    "UNSCR 1636 (2005))"
)


def check_empty_annex(roman: str, block: Element) -> None:
    """Accept the never-used list: its subtitle and one note paragraph."""
    subtitles = 0
    placeholders = 0
    for child in block.iterchildren():
        if not isinstance(child.tag, str):
            continue
        cls = child.get("class") or ""
        if child.tag == "hr" and cls == "separator-annex":
            continue
        if child.tag == "p" and cls in SKIP_P_CLASSES:
            continue
        if child.tag == "p" and cls == "title-gr-seq-level-1":
            if clean(element_text(child), roman) != ANNEX_I_SUBTITLE:
                raise ParseError(f"{roman}: annex subtitle changed")
            subtitles += 1
            continue
        if child.tag == "p" and cls == "norm":
            if clean(element_text(child), roman) != PLACEHOLDER:
                raise ParseError(f"{roman}: empty annex now has content")
            placeholders += 1
            continue
        raise ParseError(f"{roman}: unexpected <{child.tag} class={cls!r}>")
    if subtitles != 1 or placeholders != 1:
        raise ParseError(
            f"{roman}: expected one subtitle and one placeholder, "
            f"got {subtitles} and {placeholders}"
        )


def parse_document(doc: Element) -> list[dict[str, str]]:
    for roman, block in annex_blocks(doc, {TARGET_ANNEX} | NON_TARGET):
        if roman == TARGET_ANNEX:
            check_empty_annex(roman, block)
    return []


@click.command(help="Parse consolidated Regulation (EC) 305/2006 into a CSV.")
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
        records = parse_document(doc)
        validate_records(records)
        csv_path = write_csv(records, FRAMEWORK_CELEX)
        click.echo(json.dumps(summary(records, celex), indent=2))
        click.echo(f"wrote {csv_path}")
    except ParseError as exc:
        raise click.ClickException(str(exc)) from exc


if __name__ == "__main__":
    main()
