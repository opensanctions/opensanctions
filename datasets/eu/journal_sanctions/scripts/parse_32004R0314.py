"""Parse consolidated Regulation (EC) 314/2004 (Zimbabwe) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The regulation's annexes:

- Annex I — equipment which might be used for internal repression
  (Article 3), a goods list, not designations.
- Annex II — competent-authority websites, not designations.

The consolidated text contains no designation list at all: the fund-freeze
articles and their Annex III were repealed after the last designation was
delisted, leaving only the arms-embargo and export-control provisions
(matching the EU-ZWE program's measures). This parser therefore emits zero
rows and carries no measure constant — there is nothing to attribute a
measure to. It exists to pin that reviewed state: a re-added designation
annex is an unknown annex title, and a repurposed annex is a changed
subtitle, and both break the run for review.

Output: data/consolidated/32004R0314.csv (the EU Journal consolidated CSV
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
    check_marker,
    clean,
    load_source,
    summary,
    to_record,
    validate_records,
    write_csv,
)
from lxml import html
from zavod.helpers.html import element_text
from zavod.stateful.programs import get_program_by_key
from zavod.util import Element

FRAMEWORK_CELEX = "32004R0314"
CONSOLIDATED_RE = re.compile(r"^02004R0314-\d{8}$")
PROGRAM_KEY = "EU-ZWE"

# Every annex in the document, with its printed subtitle and the content
# element it is allowed to hold. Neither annex lists designations.
ANNEXES = {
    "I": (
        "List of equipment which might be used for internal repression "
        "referred to in Article 3",
        ("div", "grid-container grid-list"),
    ),
    "II": (
        "Websites for information on the competent authorities referred to "
        "in Articles 4 and 8 and address for notifications to the European "
        "Commission",
        ("p", "norm"),
    ),
}


def check_registry() -> None:
    program = get_program_by_key(PROGRAM_KEY)
    if program is None:
        raise ParseError(f"unknown program key {PROGRAM_KEY!r}")


def check_annex(annex: str, block: Element) -> None:
    subtitle, (content_tag, content_class) = ANNEXES[annex]
    seen_subtitle = False
    seen_content = False
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
            if clean(element_text(child), annex) != subtitle or seen_subtitle:
                raise ParseError(f"{annex}: unexpected annex subtitle")
            seen_subtitle = True
            continue
        if child.tag == "p" and cls in SKIP_P_CLASSES:
            continue
        if child.tag == content_tag and cls == content_class:
            if not seen_subtitle:
                raise ParseError(f"{annex}: content before annex subtitle")
            seen_content = True
            continue
        raise ParseError(f"{annex}: unexpected <{child.tag} class={cls!r}>")
    if not seen_subtitle:
        raise ParseError(f"{annex}: missing annex subtitle")
    if not seen_content:
        raise ParseError(f"{annex}: annex has no content")


def parse_document(doc: Element) -> list[Row]:
    for annex, block in annex_blocks(doc, set(ANNEXES)):
        check_annex(annex, block)
    return []


@click.command(help="Parse consolidated Regulation 314/2004 into a CSV candidate.")
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
            raise ParseError(f"not a consolidated 314/2004 CELEX: {celex!r}")
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
