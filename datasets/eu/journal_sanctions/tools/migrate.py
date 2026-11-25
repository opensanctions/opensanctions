"""Bring op-log files up to the current column contract.

When the format gains, reorders or renames columns, this rewrites every file
with the header from `common.OPLOG_COLUMNS`: new columns start empty, renamed
ones move to their new name, and cell values and row order carry over
unchanged. A column the contract no longer knows is an error that leaves the
file untouched, so no data is dropped silently.
"""

import csv
import sys
from pathlib import Path

import click

from .common import OPLOG_COLUMNS, REGULATIONS_DIR, write_oplog

# Old column name to current column name.
RENAMES: dict[str, str] = {}


def migrate(path: Path) -> list[dict[str, str]] | None:
    """Return the file's rows under the current header, or None if current."""
    with open(path, encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh)
        header = [RENAMES.get(column, column) for column in next(reader, [])]
        unknown = [column for column in header if column not in OPLOG_COLUMNS]
        if unknown:
            raise click.ClickException(f"{path}: unknown columns {unknown}")
        if len(set(header)) != len(header):
            raise click.ClickException(f"{path}: repeated columns {header}")
        rows: list[dict[str, str]] = []
        for line, cells in enumerate(reader, start=2):
            if len(cells) != len(header):
                raise click.ClickException(
                    f"{path}:{line}: expected {len(header)} cells"
                )
            row = dict.fromkeys(OPLOG_COLUMNS, "")
            row.update(zip(header, cells))
            rows.append(row)
    if tuple(header) == OPLOG_COLUMNS:
        return None
    return rows


@click.command()
@click.argument("frameworks", nargs=-1)
@click.option("--check", is_flag=True, help="Only list the files that need it.")
def main(frameworks: tuple[str, ...], check: bool) -> None:
    """Migrate the op-log files of FRAMEWORKS (default: all of them)."""
    names = frameworks or tuple(
        sorted(p.name for p in REGULATIONS_DIR.iterdir() if p.is_dir())
    )
    pending = 0
    for framework in names:
        for path in sorted((REGULATIONS_DIR / framework).glob("*.csv")):
            rows = migrate(path)
            if rows is None:
                continue
            pending += 1
            if check:
                click.echo(f"needs migrating: {path}")
            else:
                write_oplog(path, rows)
                click.echo(f"migrated: {path}")
    sys.exit(1 if check and pending else 0)


if __name__ == "__main__":
    main()
