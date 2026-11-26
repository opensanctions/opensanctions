"""Rewrite designation ids, and pair designations with their published ids.

`rewrite-ids` changes a stack's `id` cells by an explicit `old,new` mapping.
`legacy-ids` computes that mapping for a framework from the consolidated
snapshot and amendment files the pre-op-log crawler publishes, so each
designation keeps the entity ID it is published under. It reads data that
goes away when the crawler switches to the op-log.
"""

import csv
import re
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any

import click
import yaml

from .common import DATASET_DIR, mint_id, read_oplog, regime_files, write_oplog
from .replay import Entry, replay

ID_RE = re.compile(r"^[0-9a-f]{40}$")
DATA_DIR = DATASET_DIR / "data"


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


def _published_rows(framework: str) -> list[tuple[str, dict[str, str]]]:
    """The rows the pre-op-log crawler emits for a framework, as (source, row)."""
    config = _config()
    pin: str = config["consolidation"][framework]
    absorbed: dict[str, str] = config.get("consolidated_amendments", {})
    rows: list[tuple[str, dict[str, str]]] = []
    with open(DATA_DIR / "consolidated" / f"{framework}.csv", encoding="utf-8") as fh:
        rows.extend(("consolidated", row) for row in csv.DictReader(fh))
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
    pin: str = _config()["consolidation"][framework]
    as_of = date.fromisoformat(pin.rsplit("-", 1)[-1]).isoformat()
    at_pin = list(replay(framework, as_of=as_of).live.values())
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


@click.command("legacy-ids")
@click.argument("framework")
@click.option(
    "--out",
    type=click.Path(dir_okay=False, path_type=Path),
    help="Write the `old,new` mapping here, for `rewrite-ids`.",
)
def legacy_command(framework: str, out: Path | None) -> None:
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
