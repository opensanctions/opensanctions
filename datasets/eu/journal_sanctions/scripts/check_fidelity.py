"""Check that a consolidated CSV's values occur in the source act it cites.

``validate.py`` checks the reviewed CSVs against the column contract in
``../data/FORMAT.md`` only — it cannot tell whether a cell actually matches
the source act, and says so. This tool closes that gap for consolidated
files: it fetches the pinned CELLAR expression named in the dataset YAML's
``config.consolidation`` for a framework, and checks that every transcribed
value is a literal substring of that document's text.

Checked columns are every column in the contract except ``programKey``,
``measure``, and ``schema`` (classifications the reviewer assigns, never
printed by the source) and ``celex`` (the framework's own identifier, which
the source act does not print in that form — checking it would fail on
every row and add no signal). Multi-valued cells are decoded with
``split_multi`` and each element is checked on its own.

This is a substring check on document text collapsed to single spaces, not a
structural one: it can miss a value the source prints with typography the
document text does not preserve (e.g. a line-wrap that drops a hyphen), and
it can pass a short value that merely happens to occur elsewhere in the
act. It cannot confirm a value is correct, only flag one that is not found at
all — a strong signal that transcription drifted, or that the pinned
version in the dataset YAML is stale.

Usage, from the dataset directory:

    python scripts/check_fidelity.py [CSV ...]

With no arguments it checks every CSV under ``data/consolidated/``. Exits `1`
if any value is not found, `0` otherwise. Fetched expressions are cached in
``scripts/out/`` like the parsers cache them, so a repeat run is offline.
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path

import click
import yaml
from lxml import html

from common import (
    CONSOLIDATED_COLUMNS,
    DATASET_DIR,
    ENTITY_COLUMNS,
    INLINE_MARKER_RE,
    OUTPUT_DIR,
    load_source,
    split_multi,
)
from zavod.helpers.html import element_text
from zavod.shed.ojeu.celex import eur_lex_url

# Standalone modification-reference paragraphs ("▼M37", "▼M12 ————") become
# whitespace-delimited tokens once the document is squashed to one line; drop
# them the same way ``common.clean`` drops the inline ►Cn ... ◄ markers.
STANDALONE_MARKER_RE = re.compile(r"▼(?:B|C\d+|M\d+)(?:\s+—+)?")

# Not printed by the source in this form; see module docstring.
UNCHECKED_COLUMNS = frozenset({"celex", "programKey", "measure", "schema"})
CHECKED_COLUMNS = tuple(c for c in CONSOLIDATED_COLUMNS if c not in UNCHECKED_COLUMNS)
MULTI_VALUE_CHECKED = frozenset(CHECKED_COLUMNS) & (
    frozenset(ENTITY_COLUMNS) - {"name"}
)

YAML_PATH = DATASET_DIR / "eu_journal_sanctions.yml"


@dataclass(frozen=True)
class Issue:
    file: Path
    row: int
    column: str
    value: str
    message: str

    def __str__(self) -> str:
        return f"{self.file}:{self.row}:{self.column}: {self.message}: {self.value!r}"


def load_consolidation_pins() -> dict[str, str]:
    """Read `config.consolidation` (framework CELEX -> pinned version CELEX)."""
    with open(YAML_PATH, encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    pins = data["config"]["consolidation"]
    if not isinstance(pins, dict):
        raise click.ClickException(
            f"{YAML_PATH}: config.consolidation is not a mapping"
        )
    return {str(k): str(v) for k, v in pins.items()}


def document_text(content: bytes) -> str:
    """Full document text, modification markers stripped, whitespace collapsed."""
    doc = html.fromstring(content)
    text = element_text(doc)
    text = INLINE_MARKER_RE.sub(" ", text)
    text = STANDALONE_MARKER_RE.sub(" ", text)
    return " ".join(text.split())


def read_records(path: Path) -> list[dict[str, str]]:
    import csv

    with open(path, encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if (
            reader.fieldnames is None
            or tuple(reader.fieldnames) != CONSOLIDATED_COLUMNS
        ):
            raise click.ClickException(
                f"{path}: header does not match the consolidated CSV contract; "
                "run validate.py first"
            )
        return list(reader)


def row_values(record: dict[str, str], column: str) -> list[str]:
    value = record[column]
    if value == "":
        return []
    if column == "annex":
        # A compact join of the source's own annex and part identifiers
        # ("I.A" for "ANNEX I" part "A"), not a string the source prints as
        # one run; check the parts, which are printed, rather than the join.
        return value.split(".")
    if column in MULTI_VALUE_CHECKED:
        try:
            return split_multi(value)
        except ValueError:
            # A malformed multi-value cell is validate.py's job to report;
            # skip it here rather than raise on a decode error.
            return []
    return [value]


def check_file(path: Path, pins: dict[str, str], source: Path | None) -> list[Issue]:
    framework_celex = path.stem
    records = read_records(path)

    mismatched_celex = {r["celex"] for r in records} - {framework_celex}
    if mismatched_celex:
        raise click.ClickException(
            f"{path}: row celex {sorted(mismatched_celex)} does not match "
            f"filename {framework_celex!r}; run validate.py first"
        )

    if not records:
        return []

    pinned = pins.get(framework_celex)
    if pinned is None:
        raise click.ClickException(
            f"{path}: no config.consolidation pin for {framework_celex} in "
            f"{YAML_PATH.name}"
        )

    click.echo(f"fetching {eur_lex_url(pinned)}")
    content = load_source(pinned, source)
    text = document_text(content)

    issues: list[Issue] = []
    for row_num, record in enumerate(records, start=2):
        for column in CHECKED_COLUMNS:
            for value in row_values(record, column):
                if value not in text:
                    issues.append(
                        Issue(path, row_num, column, value, "not found in source text")
                    )
    return issues


@click.command(
    help="Check that consolidated CSV values occur in their pinned source act."
)
@click.argument(
    "paths",
    nargs=-1,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
)
@click.option(
    "--source",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
    help="Check against these exact XHTML bytes instead of fetching from CELLAR. "
    "Only valid with exactly one CSV path.",
)
def cli(paths: tuple[Path, ...], source: Path | None) -> None:
    if len(paths) == 0:
        paths = tuple(sorted(OUTPUT_DIR.glob("*.csv")))
        if len(paths) == 0:
            raise click.UsageError(f"No CSV files found under {OUTPUT_DIR}")
    if source is not None and len(paths) != 1:
        raise click.UsageError("--source requires exactly one CSV path")

    pins = load_consolidation_pins()

    all_issues: list[Issue] = []
    for path in paths:
        issues = check_file(path, pins, source)
        all_issues.extend(issues)
        if len(issues) == 0:
            click.echo(f"{path}: OK")
        else:
            click.echo(f"{path}: {len(issues)} value(s) not found in source")
    for issue in all_issues:
        click.echo(str(issue), err=True)
    if len(all_issues) > 0:
        sys.exit(1)


if __name__ == "__main__":
    cli()
