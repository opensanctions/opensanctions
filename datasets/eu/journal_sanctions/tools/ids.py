"""Rewrite designation ids by an explicit mapping.

`rewrite-ids` changes a stack's `id` cells by an `old,new` mapping, all or
nothing; `legacy-ids` (in `legacy.py`) computes one that gives designations
the entity IDs the pre-op-log crawler publishes.
"""

import csv
import re
from pathlib import Path

import click

from .common import read_oplog, regime_files, write_oplog

ID_RE = re.compile(r"^[0-9a-f]{40}$")


def rewrite_ids(framework: str, mapping: dict[str, str]) -> dict[Path, int]:
    """Rewrite `id` cells across a framework's files; return rows changed per file.

    Raises:
        click.ClickException: An old id is not in the stack, a new id is
            malformed or already taken, or two old ids map to one new id.
            Nothing is written then.
    """
    files = {path: read_oplog(path) for path in regime_files(framework)}
    present = {row["id"] for rows in files.values() for row in rows}
    problems = [f"{old} is not in the stack" for old in mapping if old not in present]
    problems += [
        f"{new} is not 40 hex" for new in mapping.values() if not ID_RE.match(new)
    ]
    targets = list(mapping.values())
    problems += [
        f"{new} is a target twice" for new in set(targets) if targets.count(new) > 1
    ]
    problems += [
        f"{new} is already used in the stack"
        for new in targets
        if new in present and new not in mapping
    ]
    if problems:
        raise click.ClickException("; ".join(sorted(problems)))
    changed: dict[Path, int] = {}
    for path, rows in files.items():
        count = 0
        for row in rows:
            new = mapping.get(row["id"])
            if new is not None:
                row["id"] = new
                count += 1
        if count:
            write_oplog(path, rows)
            changed[path] = count
    return changed


@click.command("rewrite-ids")
@click.argument("framework")
@click.argument("mapping", type=click.Path(exists=True, path_type=Path))
def rewrite_command(framework: str, mapping: Path) -> None:
    """Rewrite FRAMEWORK's ids by MAPPING, a CSV with `old,new` columns."""
    with open(mapping, encoding="utf-8") as fh:
        pairs = {row["old"]: row["new"] for row in csv.DictReader(fh)}
    changed = rewrite_ids(framework, pairs)
    for path, count in sorted(changed.items()):
        click.echo(f"{path.name}: {count} rows")
    click.echo(
        f"{len(pairs)} ids, {sum(changed.values())} rows in {len(changed)} files"
    )
