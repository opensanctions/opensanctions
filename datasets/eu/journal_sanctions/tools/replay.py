"""Replay a framework's op-log files into the state of its designations.

Files apply in `(effectiveDate, publishedDate, celex)` order, rows in file
order. The result holds every designation that is live, and every one that
was deleted, under its key `(id, programKey, measure)`, together with the
dates the crawler derives: first listing, entry into force, last change.
"""

import csv
import json
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
    parse_date,
    read_oplog,
    regime_files,
)

CONSOLIDATION_RE = re.compile(r"^0\d{4}[A-Z]\d{4}-(\d{4})(\d{2})(\d{2})$")
# Header lines of a EUR-Lex consolidation: "►M2 | COUNCIL IMPLEMENTING
# REGULATION (EU) 2017/1568 of …" and "►C1 | Corrigendum, OJ L 251, …
# (2017/1568)".
AMENDED_RE = re.compile(
    r"^►M\d+ \| .*?(REGULATION|DECISION)\b[^|]*?\([A-Za-z]+\) (?:No )?(\d+)/(\d+)",
    re.IGNORECASE,
)
CORRECTED_RE = re.compile(r"^►C\d+ \| Corrigendum.*\((?:No )?(\d+)/(\d+)\)")

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
            f"{celex} has no file; run `python -m tools.act {celex}` first"
        )
    meta = json.loads(path.read_text())
    published = meta["publication_date"]
    return (min(meta["entry_into_force"] or [published]), published, celex)


def replay(
    framework: str, before: str | None = None, as_of: str | None = None
) -> State:
    """Replay a framework's ops.

    Args:
        before: Stop ahead of this act, to see what it amends. An act without
            a file yet is placed by the dates in its fetched `meta.json`.
        as_of: Apply only ops with `effectiveDate` on or before this ISO date,
            and move designations whose `endDate` is before it to `lapsed`.
    """
    files = load_ops(framework)
    stop: tuple[str, str, str] | None = None
    if before is not None and before not in {celex for celex, _ in files}:
        stop = act_position(before)
    state = State()
    for celex, ops in files:
        if celex == before:
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


def cited_acts(text: str) -> tuple[set[str], dict[str, int]]:
    """Read the acts a consolidation's header lists as amending or correcting.

    Returns the CELEXes of the "Amended by" acts and, per corrected act, how
    many corrigenda the "Corrected by" list cites.
    """
    amended: set[str] = set()
    corrected: dict[str, int] = {}
    for line in text.splitlines():
        if line.startswith("▼B"):
            break
        amending = AMENDED_RE.match(line)
        if amending is not None:
            celex = act_celex(*amending.groups())
            if celex is not None:
                amended.add(celex)
            continue
        correcting = CORRECTED_RE.match(line)
        if correcting is not None:
            celex = act_celex("REGULATION", *correcting.groups())
            if celex is not None:
                corrected[celex] = corrected.get(celex, 0) + 1
    return amended, corrected


def act_celex(kind: str, first: str, second: str) -> str | None:
    """Turn an act number ("2017/1548" or "No 284/2014") into a CELEX."""
    letter = "D" if "DECISION" in kind.upper() else "R"
    if len(first) == 4 and 1950 < int(first) < 2100:
        year, number = first, second
    elif len(second) == 4:
        year, number = second, first
    else:
        return None
    return f"3{year}{letter}{int(number):04d}"


def compare(framework: str, consolidation: str) -> int:
    """Check a replay against a EUR-Lex consolidation's text; return errors.

    Every value of every live entry must be printed in the consolidation, up
    to case and the typography of quotes and dashes; a value that differs
    only in spacing is a warning. A deleted entry whose name is still printed
    is a warning, as another entry may name it. The acts and corrigenda the
    consolidation's header cites are compared with the stack's files.
    Entries printed in the consolidation but missing from the replay are not
    detected: finding entries takes reading the layout.
    """
    match = CONSOLIDATION_RE.match(consolidation)
    if match is None:
        raise click.UsageError(f"not a consolidated CELEX: {consolidation}")
    as_of = "-".join(match.group(1, 2, 3))
    directory = OUT_DIR / consolidation
    if not (directory / "text.txt").exists():
        with cli_client() as client:
            fetch_act(client, consolidation, directory)
    raw = (directory / "text.txt").read_text(encoding="utf-8")
    # Consolidations recase names and retypeset quotes, so match loosely.
    text = haystack(raw, loose=True)
    squeezed = "".join(text.split())
    state = replay(framework, as_of=as_of)
    errors = 0
    for entry in state.live.values():
        for column, value in missing_values(entry.row, text, loose=True):
            where = f"{entry.row['id'][:10]} {entry.row['name'][:40]!r}"
            if "".join(loosen(value).split()) in squeezed:
                click.echo(
                    f"WARNING  {where}: {column} differs only in spacing: {value[:60]!r}"
                )
                continue
            errors += 1
            click.echo(
                f"ERROR    {where}: {column} not in {consolidation}: {value[:60]!r}"
            )
    for entry in state.deleted.values():
        if loosen(" ".join(entry.row["name"].split())) in text:
            click.echo(
                f"WARNING  {entry.row['id'][:10]} deleted, but "
                f"{entry.row['name'][:40]!r} is printed in {consolidation}"
            )
    files = {path.stem for path in regime_files(framework)}
    amended, corrected = cited_acts(raw)
    for celex in sorted(amended - files):
        errors += 1
        click.echo(f"ERROR    {consolidation} is amended by {celex}, which has no file")
    for celex, count in sorted(corrected.items()):
        have = sum(1 for stem in files if stem.startswith(f"{celex}R("))
        if have != count:
            errors += 1
            click.echo(
                f"ERROR    {consolidation} cites {count} corrigenda of {celex}; "
                f"the stack has {have}"
            )
    click.echo(
        f"{consolidation}: {len(state.live)} live entries as of {as_of}, "
        f"{len(state.lapsed)} lapsed, {errors} errors"
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
