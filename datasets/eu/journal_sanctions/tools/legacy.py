"""Compare a framework's op-log with the data the pre-op-log crawler publishes.

The crawler still emits `data/consolidated/` snapshots and `data/amendments/`
files. These commands pair them with the op-log while frameworks are being
migrated (see ../MIGRATION.md), and go away with that data:

- `legacy-ids` pairs each designation with the entity ID it is published
  under, as a mapping for `rewrite-ids`;
- `legacy-compare` reports the delta between the replay and the snapshot;
- `legacy-status` lists every pinned framework's migration progress.
"""

import csv
import re
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path
from typing import Any

import click
import yaml

from .common import (
    DATASET_DIR,
    mint_id,
    regime_files,
    related_acts,
    split_multi,
)
from .replay import Entry, replay

DATA_DIR = DATASET_DIR / "data"
# Old-snapshot columns that are not about the designee's description.
SKIP_COLUMNS = {
    "celex",
    "amendedCelex",
    "amendmentCelex",
    "recordId",
    "programKey",
    "annex",
    "measure",
    "startDate",
    "schema",
    "ethnicity",
}
WORD_RE = re.compile(r"[^\W\d_]{3,}", re.U)
# Words the op-log drops on purpose: printed labels, document metadata and
# review history (FORMAT.md principle 5).
DISCOUNTED = set(
    """
    alias aka also known formerly previously listed born nee née name names
    number numbers identification registration registered tax taxpayer tin
    inn kpp ogrn okpo psrn imo swift bic passport card national document
    issued issue expires expired expiry expiration valid until date dated
    cancelled officially review pursuant resolution security council was
    concluded jan feb mar apr may jun jul aug sep sept oct nov dec january
    february march april june july august september october november december
    type entity place business principal email website telephone phone fax
    mobile gender pob dob nationality text image russian ukrainian chinese
    korean farsi arabic english turkish belarusian kazakh azerbaijani rusian
    and the for with
    """.split()
)


def _norm(name: str) -> str:
    return " ".join(re.sub(r"[\"'‘’“”„«»`]", "", name).casefold().split())


def _match(old: dict[str, str], live: list[Entry]) -> Entry | None:
    """The live designation an old row describes: same annex and number,
    else the same name in the annex."""
    in_annex = [e for e in live if e.row["annex"] == old["annex"]]
    by_number = [e for e in in_annex if e.row["recordId"] == old["recordId"]]
    if old["recordId"] != "" and len(by_number) == 1:
        return by_number[0]
    candidates = by_number or in_annex
    name = _norm(old["name"])
    by_name = [e for e in candidates if _norm(e.row["name"]) == name]
    if len(by_name) == 1:
        return by_name[0]
    # The old parser kept aliases printed after the name in it:
    # "O Rang (Po Thong Gang)", "… Ltd (aka …) - شركة …".
    by_head = [
        e
        for e in candidates
        if name.startswith((_norm(e.row["name"]) + " (", _norm(e.row["name"]) + " -"))
    ]
    if len(by_head) == 1:
        return by_head[0]
    return None


def _config() -> dict[str, Any]:
    text = (DATASET_DIR / "eu_journal_sanctions.yml").read_text(encoding="utf-8")
    config: dict[str, Any] = yaml.safe_load(text)["config"]
    return config


def _pin_date(framework: str) -> str:
    pin: str = _config()["consolidation"][framework]
    return date.fromisoformat(pin.rsplit("-", 1)[-1]).isoformat()


def _snapshot(framework: str) -> list[dict[str, str]]:
    with open(DATA_DIR / "consolidated" / f"{framework}.csv", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _published_rows(framework: str) -> list[tuple[str, dict[str, str]]]:
    """The rows the pre-op-log crawler emits for a framework, as (source, row)."""
    config = _config()
    pin: str = config["consolidation"][framework]
    absorbed: dict[str, str] = config.get("consolidated_amendments", {})
    rows = [("consolidated", row) for row in _snapshot(framework)]
    for path in sorted((DATA_DIR / "amendments").glob("*.csv")):
        if absorbed.get(path.stem) == pin:
            continue
        with open(path, encoding="utf-8") as fh:
            rows.extend(
                (path.stem, row)
                for row in csv.DictReader(fh)
                if row["amendedCelex"] == framework
            )
    return rows


def legacy_ids(framework: str) -> tuple[dict[str, str], list[str]]:
    """Map each live designation's `id` to the id the old crawler publishes.

    Returns the `{current: published}` pairs that differ, and the problems
    found: old rows matching no designation, and designations claimed twice.
    """
    at_pin = list(replay(framework, as_of=_pin_date(framework)).live.values())
    latest = list(replay(framework).live.values())
    claimed: dict[str, set[str]] = defaultdict(set)
    problems: list[str] = []
    for source, old in _published_rows(framework):
        entry = _match(old, at_pin if source == "consolidated" else latest)
        if entry is None:
            problems.append(
                f"{source}: no designation for {old['annex']} "
                f"{old['recordId']!r} {old['name'][:50]!r}"
            )
            continue
        claimed[entry.row["id"]].add(mint_id(framework, old["recordId"], old["name"]))
    mapping: dict[str, str] = {}
    for current, published in sorted(claimed.items()):
        if len(published) > 1:
            problems.append(f"{current} is published under {sorted(published)}")
            continue
        (target,) = published
        if target != current:
            mapping[current] = target
    return mapping, problems


def _values(row: dict[str, str], column: str) -> set[str]:
    cell = row.get(column, "")
    if cell == "":
        return set()
    values = [cell] if column == "name" else split_multi(cell)
    return {" ".join(v.split()) for v in values}


def _loose(values: set[str]) -> set[str]:
    return {_norm(v).rstrip(". ") for v in values}


def _words(row: dict[str, str]) -> set[str]:
    found: set[str] = set()
    for column, cell in row.items():
        if column in SKIP_COLUMNS or column in ("op", "id") or cell == "":
            continue
        found.update(w.casefold() for w in WORD_RE.findall(cell))
    return found


def legacy_compare(framework: str, details: bool = False) -> int:
    """Print the delta between the replay at the pin and the old snapshot.

    Returns the number of snapshot rows that match no designation.
    """
    as_of = _pin_date(framework)
    old_rows = _snapshot(framework)
    live = list(replay(framework, as_of=as_of).live.values())
    click.echo(f"{framework} as of {as_of}: {len(old_rows)} old rows, {len(live)} live")
    old_parts = Counter(row["annex"] for row in old_rows)
    new_parts = Counter(e.row["annex"] for e in live)
    for part in sorted(set(old_parts) | set(new_parts)):
        mark = "" if old_parts[part] == new_parts[part] else "  <-"
        click.echo(
            f"  {part:14} old {old_parts[part]:5}  op-log {new_parts[part]:5}{mark}"
        )

    pairs: list[tuple[dict[str, str], dict[str, str]]] = []
    seen: set[str] = set()
    unmatched = 0
    for old in old_rows:
        entry = _match(old, live)
        if entry is None:
            unmatched += 1
            click.echo(
                f"UNMATCHED old {old['annex']} {old['recordId']!r} {old['name'][:60]!r}"
            )
            continue
        seen.add(entry.row["id"])
        pairs.append((old, entry.row))
    for entry in live:
        if entry.row["id"] not in seen:
            click.echo(
                f"UNMATCHED op-log {entry.row['annex']} {entry.row['recordId']!r} "
                f"{entry.row['name'][:60]!r}"
            )

    same_id = sum(
        mint_id(framework, o["recordId"], o["name"]) == n["id"] for o, n in pairs
    )
    click.echo(f"ids: {same_id} as published, {len(pairs) - same_id} not")
    schemata = Counter(
        (o["schema"], n["schema"]) for o, n in pairs if o["schema"] != n["schema"]
    )
    for (before, after), count in schemata.most_common():
        click.echo(f"schema: {before} -> {after}: {count}")

    columns = [c for c in old_rows[0] if c not in SKIP_COLUMNS] if old_rows else []
    click.echo(f"{'column':20} {'filled':>6} {'exact':>6} {'loose':>6}")
    for column in columns:
        filled = exact = loose = 0
        for old, new in pairs:
            a, b = _values(old, column), _values(new, column)
            if not a and not b:
                continue
            filled += 1
            exact += a == b
            loose += _loose(a) == _loose(b)
        if filled:
            click.echo(f"{column:20} {filled:6} {exact:6} {loose:6}")

    lost = Counter[str]()
    entries = 0
    for old, new in pairs:
        only = {w for w in _words(old) - _words(new) if w not in DISCOUNTED}
        if not only:
            continue
        entries += 1
        lost.update(only)
        if details:
            click.echo(f"OLD ONLY {new['name'][:40]!r}: {' '.join(sorted(only))}")
    common = ", ".join(f"{w} ({n})" for w, n in lost.most_common(15))
    click.echo(f"entries with words only in the old snapshot: {entries}; {common}")
    return unmatched


def _status(framework: str) -> dict[str, str]:
    rows = _snapshot(framework)
    status = {
        "framework": framework,
        "program": ", ".join(sorted({r["programKey"] for r in rows})),
        "pin": _pin_date(framework),
        "entries": str(len(rows)),
        "acts": "",
        "newest": "",
        "state": "not started",
        "ids": "",
    }
    acts = related_acts(framework)
    try:
        filed = {path.stem for path in regime_files(framework)}
    except ValueError:
        filed = set()
    done = [a for a in acts if a.celex in filed]
    status["acts"] = f"{len(done)}/{len(acts)}"
    if not filed:
        return status
    status["newest"] = max((a.celex for a in done), default="")
    if len(done) < len(acts):
        status["state"] = "in progress"
        return status
    status["state"] = "transcribed"
    mapping, problems = legacy_ids(framework)
    if problems:
        status["ids"] = f"{len(problems)} unmatched"
    elif mapping:
        status["ids"] = f"{len(mapping)} to rewrite"
    else:
        status["ids"] = "backported"
    return status


@click.command("legacy-ids")
@click.argument("framework")
@click.option(
    "--out",
    type=click.Path(dir_okay=False, path_type=Path),
    help="Write the `old,new` mapping here, for `rewrite-ids`.",
)
def legacy_ids_command(framework: str, out: Path | None) -> None:
    """Pair FRAMEWORK's designations with the ids the current crawler publishes."""
    mapping, problems = legacy_ids(framework)
    for problem in problems:
        click.echo(f"ERROR    {problem}")
    if out is not None:
        with open(out, "w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh, lineterminator="\n")
            writer.writerow(["old", "new"])
            writer.writerows(sorted(mapping.items()))
    click.echo(f"{framework}: {len(mapping)} ids to rewrite, {len(problems)} errors")
    if problems:
        raise SystemExit(1)


@click.command("legacy-compare")
@click.argument("framework")
@click.option("--details", is_flag=True, help="List the old-only words per entry.")
def legacy_compare_command(framework: str, details: bool) -> None:
    """Compare FRAMEWORK's replay at its pin with the old consolidated snapshot."""
    if legacy_compare(framework, details=details):
        raise SystemExit(1)


@click.command("legacy-status")
@click.option("--markdown", is_flag=True, help="Print a Markdown table.")
def legacy_status_command(markdown: bool) -> None:
    """List every pinned framework's migration progress."""
    rows = [_status(framework) for framework in sorted(_config()["consolidation"])]
    header = [
        "framework",
        "program",
        "pin",
        "entries",
        "acts",
        "newest",
        "state",
        "ids",
    ]
    if markdown:
        click.echo("| " + " | ".join(header) + " |")
        click.echo("|" + " --- |" * len(header))
        for row in rows:
            click.echo("| " + " | ".join(row[h] for h in header) + " |")
        return
    for row in rows:
        click.echo("  ".join(f"{row[h]:<12}" for h in header))
