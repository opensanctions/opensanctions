"""Parse consolidated Decision 2011/173/CFSP (Bosnia and Herzegovina) into a CSV.

This parser runs inside an agentic harness that fixes it when the source
evolves. It must therefore break on any structure it has not been taught,
with a short error message naming the annex and the problem, so the harness
can read the error and update this file. Never widen a rule beyond the
formats actually observed in the document; a new format is a code review
event, not a fallback case.

The framework act is a CFSP decision: no implementing regulation was ever
adopted for this regime, so the decision carries the Article 2 asset freeze
itself. The decision has a single unnumbered annex ("ANNEX", the list of
natural and legal persons referred to in Articles 1 and 2) which has never
held a designation — it prints one "…" placeholder paragraph. The parser
accepts exactly that placeholder and breaks the day the Council designates
anyone, so the entry structure gets taught under review. The snapshot is
therefore a header-only CSV: consolidated files may hold zero data rows.

Output: data/consolidated/32011D0173.csv (the EU Journal consolidated CSV
contract, keyed by the framework act). The consolidated version the snapshot
was extracted from is passed as the CELEX argument and pinned in the dataset
YAML's `consolidation` lookup, updated in the same commit as the CSV.
"""

from __future__ import annotations

import json
from pathlib import Path

import click
from common import (
    SKIP_P_CLASSES,
    ParseError,
    check_consolidated_celex,
    check_registry,
    clean,
    load_source,
    summary,
    validate_records,
    write_csv,
)
from lxml import html
from zavod.helpers.html import element_text, xpath_elements
from zavod.util import Element

FRAMEWORK_CELEX = "32011D0173"
PROGRAM_KEY = "EU-BOSNIA"
# The decision's Article 2 fund freeze; the Article 1 travel ban would be a
# second legal context per designation, but the annex has never listed one.
MEASURE = "Asset freeze"

# The document's only annex is unnumbered — the title prints as a bare
# "ANNEX" (common.annex_blocks requires an identifier, so it does not apply).
ANNEX_TITLE = "ANNEX"
ANNEX_SUBTITLE = "List of natural and legal persons referred to in Articles 1 and 2"
# The never-used list body: one paragraph holding a single horizontal
# ellipsis.
PLACEHOLDER = "…"


def annex_block(doc: Element) -> Element:
    """Locate the decision's single unnumbered annex block."""
    titles = xpath_elements(doc, "//p[@class='title-annex-1']")
    if len(titles) != 1:
        raise ParseError(f"expected one annex title, found {len(titles)}")
    text = clean(element_text(titles[0]), "annex title")
    if text != ANNEX_TITLE:
        raise ParseError(f"unrecognized annex title {text!r}")
    parent = titles[0].getparent()
    if parent is None:
        raise ParseError("annex title has no container")
    return parent


def check_empty_annex(block: Element) -> None:
    """Accept the never-used list: its subtitle and one "…" placeholder."""
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
            if clean(element_text(child), "annex") != ANNEX_SUBTITLE:
                raise ParseError("annex subtitle changed")
            subtitles += 1
            continue
        if child.tag == "p" and cls == "norm":
            if clean(element_text(child), "annex") != PLACEHOLDER:
                raise ParseError("empty annex now has content")
            placeholders += 1
            continue
        raise ParseError(f"annex: unexpected <{child.tag} class={cls!r}>")
    if subtitles != 1 or placeholders != 1:
        raise ParseError(
            f"annex: expected one subtitle and one placeholder, "
            f"got {subtitles} and {placeholders}"
        )


def parse_document(doc: Element) -> list[dict[str, str]]:
    check_empty_annex(annex_block(doc))
    return []


@click.command(help="Parse consolidated Decision 2011/173/CFSP into a CSV candidate.")
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
