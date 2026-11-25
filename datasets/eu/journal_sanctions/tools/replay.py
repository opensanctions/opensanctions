"""Replay a framework's op-log files into the state of its designations.

Files apply in `(effectiveDate, publishedDate, celex)` order, rows in file
order. The result holds every designation that is live, and every one that
was deleted, under its key `(id, programKey, measure)`, together with the
dates the crawler derives: first listing, entry into force, last change.
"""

import csv
import re
import sys
from dataclasses import dataclass, field

import click
from zavod.shed.ojeu.cellar import cli_client

from .act import OUT_DIR, fetch_act
from .common import (
    ENTITY_COLUMNS,
    haystack,
    loosen,
    missing_values,
    read_oplog,
    regime_files,
)

CONSOLIDATION_RE = re.compile(r"^0\d{4}[A-Z]\d{4}-(\d{4})(\d{2})(\d{2})$")

Key = tuple[str, str, str]


@dataclass
class Entry:
    """One designation's current row and the dates the op-log implies."""

    row: dict[str, str]
    listed_published: str
    listed_effective: str
    modified: str | None = None
    celexes: list[str] = field(default_factory=list)


@dataclass
class Op:
    """One op-log row with its provenance."""

    celex: str
    line: int
    row: dict[str, str]

    @property
    def key(self) -> Key:
        return (self.row["id"], self.row["programKey"], self.row["measure"])

    @property
    def where(self) -> str:
        return f"{self.celex}:{self.line}"


@dataclass
class State:
    live: dict[Key, Entry] = field(default_factory=dict)
    deleted: dict[Key, Entry] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    schemata: dict[str, str] = field(default_factory=dict)


def load_ops(framework: str) -> list[tuple[str, list[Op]]]:
    """Read a framework's files, in replay order, as `(celex, ops)` pairs."""
    loaded: list[tuple[tuple[str, str, str], str, list[Op]]] = []
    for path in regime_files(framework):
        rows = read_oplog(path)
        ops = [Op(path.stem, line, row) for line, row in enumerate(rows, start=2)]
        effective = min((op.row["effectiveDate"] for op in ops), default="")
        published = min((op.row["publishedDate"] for op in ops), default="")
        loaded.append(((effective, published, path.stem), path.stem, ops))
    loaded.sort(key=lambda item: item[0])
    return [(celex, ops) for _, celex, ops in loaded]


def apply(state: State, op: Op) -> None:
    """Apply one op, recording an error instead of applying an invalid one."""
    row, key = op.row, op.key
    known = state.schemata.setdefault(row["id"], row["schema"])
    if row["schema"] != known:
        state.errors.append(
            f"{op.where}: id {row['id']} was {known}, now {row['schema']}"
        )
    if row["op"] == "ADD":
        if key in state.live:
            state.errors.append(f"{op.where}: ADD of live {key}")
            return
        state.deleted.pop(key, None)
        state.live[key] = Entry(
            row=dict(row),
            listed_published=row["publishedDate"],
            listed_effective=row["effectiveDate"],
            celexes=[op.celex],
        )
    elif row["op"] == "MOD":
        entry = state.live.get(key)
        if entry is None:
            state.errors.append(f"{op.where}: MOD of missing {key}")
            return
        entry.row = dict(row)
        entry.modified = row["publishedDate"]
        entry.celexes.append(op.celex)
    elif row["op"] == "DEL":
        entry = state.live.get(key)
        if entry is None:
            state.errors.append(f"{op.where}: DEL of missing {key}")
            return
        for column in ("name", "schema"):
            if row[column] != entry.row[column]:
                state.errors.append(
                    f"{op.where}: DEL {column} {row[column]!r} does not match "
                    f"the live {entry.row[column]!r}"
                )
        entry.celexes.append(op.celex)
        state.deleted[key] = state.live.pop(key)
    else:
        state.errors.append(f"{op.where}: unknown op {row['op']!r}")


def replay(
    framework: str, before: str | None = None, as_of: str | None = None
) -> State:
    """Replay a framework's ops.

    Args:
        before: Stop ahead of this act's file, to see what it amends.
        as_of: Apply only ops with `effectiveDate` on or before this ISO date.
    """
    state = State()
    for celex, ops in load_ops(framework):
        if celex == before:
            break
        for op in ops:
            if as_of is not None and op.row["effectiveDate"] > as_of:
                continue
            apply(state, op)
    return state


def compare(framework: str, consolidation: str) -> int:
    """Check a replay against a EUR-Lex consolidation's text; return errors.

    Every value of every live entry must be printed in the consolidation,
    up to case and the typography of quotes and dashes.
    A deleted entry whose name is still printed is a warning, as another
    entry may name it. Entries printed in the consolidation but missing from
    the replay are not detected: finding entries takes reading the layout.
    """
    match = CONSOLIDATION_RE.match(consolidation)
    if match is None:
        raise click.UsageError(f"not a consolidated CELEX: {consolidation}")
    as_of = "-".join(match.group(1, 2, 3))
    directory = OUT_DIR / consolidation
    if not (directory / "text.txt").exists():
        with cli_client() as client:
            fetch_act(client, consolidation, directory)
    # Consolidations recase names and retypeset quotes, so match loosely.
    text = haystack((directory / "text.txt").read_text(encoding="utf-8"), loose=True)
    state = replay(framework, as_of=as_of)
    errors = 0
    for entry in state.live.values():
        for column, value in missing_values(entry.row, text, loose=True):
            errors += 1
            click.echo(
                f"ERROR    {entry.row['id'][:10]} {entry.row['name'][:40]!r}: "
                f"{column} not in {consolidation}: {value[:60]!r}"
            )
    for entry in state.deleted.values():
        if loosen(" ".join(entry.row["name"].split())) in text:
            click.echo(
                f"WARNING  {entry.row['id'][:10]} deleted, but "
                f"{entry.row['name'][:40]!r} is printed in {consolidation}"
            )
    click.echo(
        f"{consolidation}: {len(state.live)} live entries as of {as_of}, "
        f"{errors} values missing"
    )
    return errors


@click.command()
@click.argument("framework")
@click.option("--as-of", default=None, help="ISO date; default is every op.")
@click.option(
    "--compare",
    "consolidation",
    default=None,
    help="A consolidated CELEX (02014R0269-20140512): check the replay as of "
    "its date against its text instead of writing CSV. Entries the "
    "consolidation prints but the replay lacks are not detected; check those "
    "by reading.",
)
def main(framework: str, as_of: str | None, consolidation: str | None) -> None:
    """Write the live designations of FRAMEWORK as CSV to stdout."""
    if consolidation is not None:
        sys.exit(1 if compare(framework, consolidation) else 0)
    state = replay(framework, as_of=as_of)
    for error in state.errors:
        click.echo(error, err=True)
    columns: tuple[str, ...] = (
        "id",
        "listedPublished",
        "listedEffective",
        "modified",
        "celexes",
    )
    columns += ("recordId", "programKey", "annex", "measure", "listingDate", "schema")
    columns += ENTITY_COLUMNS + ("reason",)
    writer = csv.DictWriter(
        sys.stdout, columns, extrasaction="ignore", lineterminator="\n"
    )
    writer.writeheader()
    for entry in state.live.values():
        writer.writerow(
            {
                **entry.row,
                "listedPublished": entry.listed_published,
                "listedEffective": entry.listed_effective,
                "modified": entry.modified or "",
                "celexes": "; ".join(entry.celexes),
            }
        )
    if state.errors:
        sys.exit(1)


if __name__ == "__main__":
    main()
