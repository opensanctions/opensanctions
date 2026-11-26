"""Replay a framework's op-log files into the state of its designations.

Files apply in `(effectiveDate, publishedDate, celex)` order, rows in file
order. The result holds every designation that is live, and every one that
was deleted, under its key `(id, programKey, measure)`, together with the
dates the crawler derives: first listing, entry into force, last change.
"""

import csv
import json
import sys
from dataclasses import dataclass, field

import click

from .act import OUT_DIR
from .common import (
    ENTITY_COLUMNS,
    parse_date,
    read_oplog,
    regime_files,
)

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
    # Time-limited designations past their `endDate` in an as-of view.
    lapsed: dict[Key, Entry] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    schemata: dict[str, str] = field(default_factory=dict)


def load_ops(framework: str) -> list[tuple[str, list[Op]]]:
    """Read a framework's files, in replay order, as `(celex, ops)` pairs."""
    loaded: list[tuple[tuple[str, str, str], str, list[Op]]] = []
    for path in regime_files(framework):
        rows = read_oplog(path)
        ops = [Op(path.stem, line, row) for line, row in enumerate(rows, start=2)]
        loaded.append((file_position(path.stem, ops), path.stem, ops))
    loaded.sort(key=lambda item: item[0])
    return [(celex, ops) for _, celex, ops in loaded]


def apply(state: State, op: Op) -> None:
    """Apply one op, recording an error instead of applying an invalid one."""
    row, key = op.row, op.key
    known = state.schemata.setdefault(row["id"], row["schema"])
    # A MOD may re-register an entity under another schema.
    if row["op"] == "MOD" and key in state.live:
        state.schemata[row["id"]] = row["schema"]
    elif row["schema"] != known:
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


def file_position(celex: str, ops: list[Op]) -> tuple[str, str, str]:
    """A file's place in replay order."""
    effective = min((op.row["effectiveDate"] for op in ops), default="")
    published = min((op.row["publishedDate"] for op in ops), default="")
    return (effective, published, celex)


def act_position(celex: str) -> tuple[str, str, str]:
    """Where an act without a file yet would sit in replay order.

    Raises:
        click.UsageError: The act has not been fetched with `tools.act`.
    """
    path = OUT_DIR / celex / "meta.json"
    if not path.exists():
        raise click.UsageError(
            f"{celex} has no file; run `python -m tools act {celex}` first"
        )
    meta = json.loads(path.read_text())
    published = meta["publication_date"]
    return (min(meta["entry_into_force"] or [published]), published, celex)


def replay(
    framework: str, before: str | None = None, as_of: str | None = None
) -> State:
    """Replay a framework's ops.

    Args:
        before: Stop ahead of this act, to see what it amends. An act whose
            file is missing or header-only is placed by the dates in its
            fetched `meta.json`.
        as_of: Apply only ops with `effectiveDate` on or before this ISO date,
            and move designations whose `endDate` is before it to `lapsed`.
    """
    files = load_ops(framework)
    stop: tuple[str, str, str] | None = None
    # A header-only file has no dates of its own, so its act is placed by
    # its metadata, like an act without a file.
    if before is not None and before not in {celex for celex, ops in files if ops}:
        stop = act_position(before)
    state = State()
    for celex, ops in files:
        if celex == before and stop is None:
            break
        if stop is not None and file_position(celex, ops) >= stop:
            break
        for op in ops:
            if as_of is not None and op.row["effectiveDate"] > as_of:
                continue
            apply(state, op)
    if as_of is not None:
        for key, entry in list(state.live.items()):
            end = parse_date(entry.row["endDate"])
            if end is not None and "{:04d}-{:02d}-{:02d}".format(*end) < as_of:
                state.lapsed[key] = state.live.pop(key)
    return state


@click.command()
@click.argument("framework")
@click.option("--as-of", default=None, help="ISO date; default is every op.")
def main(framework: str, as_of: str | None) -> None:
    """Write the live designations of FRAMEWORK as CSV to stdout."""
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
