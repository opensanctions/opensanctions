"""Put an amendment CSV's header into the contract's column set and order.

Amendment files under ``data/amendments/`` are transcribed per act rather than
generated, so their header drifts: columns arrive in the order the act printed
them and columns with nothing to say get left out. This tool rewrites the file
with the exact header from ``data/FORMAT.md`` so a reviewer diffs designations
instead of column layout, and so ``validate.py`` reports substance rather than
a header mismatch that hides every other issue.

It rewrites headers only. Cell values, row order, and row content are carried
across untouched, and a column the contract does not define is an error rather
than a silent drop.
"""

import csv
import io
import sys
from pathlib import Path

import click

from common import AMENDMENT_COLUMNS, DATASET_DIR

AMENDMENT_DIR = DATASET_DIR / "data" / "amendments"


class FormatError(Exception):
    """A file the tool must not rewrite; the reviewer resolves it by hand."""


def read_text(path: Path) -> str:
    """Read the file without newline translation, so a rewrite can be compared."""
    try:
        return path.read_bytes().decode("utf-8")
    except UnicodeDecodeError as exc:
        raise FormatError(f"file is not valid UTF-8: {exc}")


def read_rows(text: str) -> list[list[str]]:
    try:
        return list(csv.reader(io.StringIO(text, newline=""), strict=True))
    except csv.Error as exc:
        raise FormatError(f"malformed CSV: {exc}")


def canonical_rows(rows: list[list[str]]) -> list[dict[str, str]]:
    """Map raw CSV rows onto the contract columns, filling absent ones empty.

    Raises ``FormatError`` for anything that cannot be remapped without
    guessing: an unknown or repeated column, or a row whose cell count does not
    match the header it was written under.
    """
    if len(rows) == 0:
        raise FormatError("file is empty")
    header = rows[0]
    unknown = [column for column in header if column not in AMENDMENT_COLUMNS]
    if len(unknown) > 0:
        raise FormatError(f"header has columns outside the contract: {unknown}")
    duplicates = sorted({column for column in header if header.count(column) > 1})
    if len(duplicates) > 0:
        raise FormatError(f"header repeats columns: {duplicates}")

    records: list[dict[str, str]] = []
    for row_num, cells in enumerate(rows[1:], start=2):
        if len(cells) != len(header):
            raise FormatError(
                f"row {row_num} has {len(cells)} cells, header has {len(header)}"
            )
        present = dict(zip(header, cells, strict=True))
        records.append(
            {column: present.get(column, "") for column in AMENDMENT_COLUMNS}
        )
    return records


def render(records: list[dict[str, str]]) -> str:
    out = io.StringIO(newline="")
    writer = csv.DictWriter(out, fieldnames=list(AMENDMENT_COLUMNS))
    writer.writeheader()
    writer.writerows(records)
    return out.getvalue()


def format_file(path: Path) -> tuple[bool, list[str]]:
    """Rewrite one amendment file in place; report whether it changed and why.

    The reasons name what the header was missing or how it was ordered, so a
    run over many files says what it did rather than only that it did it.
    """
    text = read_text(path)
    rows = read_rows(text)
    records = canonical_rows(rows)
    header = rows[0]

    reasons: list[str] = []
    missing = [column for column in AMENDMENT_COLUMNS if column not in header]
    if len(missing) > 0:
        reasons.append(f"added {len(missing)} column(s): {', '.join(missing)}")
    if header != [column for column in AMENDMENT_COLUMNS if column in header]:
        reasons.append("reordered columns")

    rendered = render(records)
    if rendered == text:
        return False, reasons
    path.write_text(rendered, encoding="utf-8", newline="")
    return True, reasons


@click.command(help="Canonicalise the header of reviewed EU Journal amendment CSVs.")
@click.argument(
    "paths",
    nargs=-1,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
)
def cli(paths: tuple[Path, ...]) -> None:
    if len(paths) == 0:
        found = sorted(AMENDMENT_DIR.glob("*.csv"))
        if len(found) == 0:
            raise click.UsageError(f"No CSV files found under {AMENDMENT_DIR}")
        paths = tuple(found)

    failed = False
    for path in paths:
        try:
            changed, reasons = format_file(path)
        except FormatError as exc:
            click.echo(f"{path}: {exc}", err=True)
            failed = True
            continue
        if changed:
            click.echo(f"{path}: rewritten ({'; '.join(reasons)})")
        else:
            click.echo(f"{path}: unchanged")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    cli()
