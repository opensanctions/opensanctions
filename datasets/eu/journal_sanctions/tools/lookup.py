"""Look up designations to amend, and resolve a draft's ids.

`entry` ranks live designations against a printed name, or prints one's
current row as the base for a MOD; `resolve` fills the `id` cells of a
draft file in place — matched for MOD/DEL rows, minted for ADD rows.
"""

import csv
import json
import sys
from pathlib import Path

import click

from .act import OUT_DIR
from .common import (
    DEL_COLUMNS,
    OPLOG_COLUMNS,
    mint_id,
    read_oplog,
    write_oplog,
)
from .match import ACCEPT, decide, describe, rank
from .replay import Entry, replay


def only(state_entries: list[Entry], column: str, given: str | None) -> str:
    """The option's value, or the one value the stack uses for that column."""
    if given is not None:
        return given
    used = sorted({entry.row[column] for entry in state_entries})
    if len(used) != 1:
        raise click.UsageError(
            f"the stack uses several {column} values {used}; pass one"
        )
    return used[0]


@click.command("entry")
@click.argument("framework")
@click.argument("id", required=False)
@click.option("--before", default=None, help="The act doing the amending.")
@click.option("--name", default=None, help="Rank live entries against this name.")
@click.option("--annex", default=None, help="With --record: the entry's annex.")
@click.option("--record", default=None, help="With --annex: the entry's recordId.")
@click.option("--schema", default="Person", help="With --name.")
@click.option(
    "--program", default=None, help="With --name; default: the stack's only one."
)
@click.option(
    "--measure", default=None, help="With --name; default: the stack's only one."
)
@click.option("--birth-date", default="", help="With --name.")
@click.option("--listing-date", default="", help="With --name.")
@click.option(
    "--as-mod",
    "as_mod",
    default=None,
    help="An act CELEX: print the row as a MOD dated like that act.",
)
@click.option(
    "--out",
    type=click.Path(path_type=Path),
    default=None,
    help="Write a complete op-log file here instead of printing.",
)
def entry(
    framework: str,
    id: str | None,
    before: str | None,
    name: str | None,
    annex: str | None,
    record: str | None,
    schema: str,
    program: str | None,
    measure: str | None,
    birth_date: str,
    listing_date: str,
    as_mod: str | None,
    out: Path | None,
) -> None:
    """Find a live designation of FRAMEWORK, or print its current row.

    With --name, rank the live designations against a printed name. With an
    id (or a prefix of one), or --annex and --record as acts address
    entries, print the entry's current row as the base for a full MOD; with
    --as-mod the row comes out as a MOD dated like that act — then change
    only what the act changes.
    """
    live = list(replay(framework, before=before or as_mod).live.values())
    if name is not None:
        query = dict.fromkeys(OPLOG_COLUMNS, "")
        query.update(
            name=name,
            schema=schema,
            programKey=only(live, "programKey", program),
            measure=only(live, "measure", measure),
            annex=annex or "",
            birthDate=birth_date,
            listingDate=listing_date,
        )
        for match in rank(query, live)[:5]:
            click.echo(describe(match))
        return
    if id is not None:
        found = [e for e in live if e.row["id"].startswith(id)]
    elif annex is not None and record is not None:
        found = [
            e for e in live if e.row["annex"] == annex and e.row["recordId"] == record
        ]
    else:
        raise click.UsageError("give --name, an id, or --annex and --record")
    if not found:
        raise click.ClickException("no live designation matches")
    if len(found) > 1:
        click.echo(f"{len(found)} live designations match", err=True)
    rows = [dict(e.row) for e in found]
    if as_mod is not None:
        meta_path = OUT_DIR / as_mod / "meta.json"
        if not meta_path.exists():
            raise click.UsageError(f"run `python -m tools act {as_mod}` first")
        meta = json.loads(meta_path.read_text())
        published = meta["publication_date"]
        effective = min(meta["entry_into_force"] or [published])
        for row in rows:
            row.update(op="MOD", publishedDate=published, effectiveDate=effective)
    if out is not None:
        write_oplog(out, rows)
        click.echo(f"{len(rows)} rows -> {out}")
        return
    writer = csv.DictWriter(sys.stdout, OPLOG_COLUMNS, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)


@click.command("resolve")
@click.argument("framework")
@click.argument("draft", type=click.Path(exists=True, path_type=Path))
@click.option("--pin", multiple=True, help="LINE=ID, for a match to set by hand.")
@click.option(
    "--delete-untouched",
    is_flag=True,
    help="The act replaces a whole list: append a DEL for every live entry it "
    "leaves out. Scope it with --annex when the act replaces one annex.",
)
@click.option("--annex", "annexes", multiple=True, help="Limit the untouched entries.")
def resolve(
    framework: str,
    draft: Path,
    pin: tuple[str, ...],
    delete_untouched: bool,
    annexes: tuple[str, ...],
) -> None:
    """Fill empty `id` cells of DRAFT in place, and check filled ones.

    MOD and DEL rows take the id, annex and recordId of the live entry they
    match; a filled MOD/DEL id is checked against the ranking. ADD rows get a
    minted id. A likely match in another annex is reported for review; a
    filled ADD id reuses it, which only an act saying so justifies. The draft's file stem names the
    act, so matching sees the state just before it. The file is written
    before the report is printed.
    """
    pins = dict(p.split("=", 1) for p in pin)
    state = replay(framework, before=draft.stem)
    live = list(state.live.values())
    by_id = {entry.row["id"]: entry for entry in live}
    rows = read_oplog(draft)
    claimed = {row["id"] for row in rows if row["id"] != ""}
    report: list[str] = []

    def settle(line: int, row: dict[str, str], entry: Entry, how: str) -> None:
        row["id"] = entry.row["id"]
        row["annex"] = entry.row["annex"]
        row["recordId"] = entry.row["recordId"]
        claimed.add(row["id"])
        report.append(f"{line} {row['op']} {row['name']} -> {how}")

    pending: dict[int, dict[str, str]] = {}
    for line, row in enumerate(rows, start=2):
        if row["op"] == "ADD":
            elsewhere = [
                m
                for m in rank(row, [e for e in live if e.row["annex"] != row["annex"]])
                if m.score >= ACCEPT
            ][:2]
            if row["id"] == "":
                row["id"] = mint_id(framework, row["recordId"], row["name"])
                report.append(f"{line} ADD minted {row['id'][:10]} {row['name']}")
            else:
                report.append(f"{line} ADD reuses {row['id'][:10]} {row['name']}")
            for match in elsewhere:
                report.append(
                    f"    listed elsewhere? {describe(match)} — a separate listing; "
                    "share its id only if the act says it is the same designee"
                )
            continue
        if row["id"] != "":
            matches = rank(row, live)
            top = matches[0].entry.row["id"] if matches else None
            if top != row["id"]:
                best = describe(matches[0]) if matches else "no candidates"
                report.append(
                    f"{line} {row['op']} CHECK {row['name']}: filled id "
                    f"{row['id'][:10]} is not the top match ({best})"
                )
            continue
        if str(line) in pins:
            settle(line, row, by_id[pins[str(line)]], "pinned")
        else:
            pending[line] = row
    # An act amends each entry once: clear matches claim their entries first,
    # and the remaining rows are ranked against what is still unclaimed.
    progress = True
    while pending and progress:
        progress = False
        for line, row in list(pending.items()):
            free = [e for e in live if e.row["id"] not in claimed]
            matches = rank(row, free)
            chosen = decide(matches)
            if chosen is None:
                continue
            runner = matches[1].score if len(matches) > 1 else 0.0
            settle(line, row, chosen.entry, f"{describe(chosen)} (next {runner:.2f})")
            del pending[line]
            progress = True
    failed = bool(pending)
    for line, row in pending.items():
        free = [e for e in live if e.row["id"] not in claimed]
        report.append(f"{line} {row['op']} UNRESOLVED {row['name']}")
        report.extend(f"    {describe(match)}" for match in rank(row, free)[:3])
    # When an act replaces a whole annex, these are the entries it drops.
    if delete_untouched and not failed and rows:
        touched = {row["id"] for row in rows}
        for target in live:
            if target.row["id"] in touched:
                continue
            if annexes and target.row["annex"] not in annexes:
                continue
            report.append(
                f"    DEL untouched {target.row['id'][:10]} "
                f"{target.row['annex']}#{target.row['recordId']} {target.row['name']}"
            )
            deletion = dict.fromkeys(OPLOG_COLUMNS, "")
            deletion.update({c: target.row[c] for c in DEL_COLUMNS})
            deletion.update(
                op="DEL",
                publishedDate=rows[0]["publishedDate"],
                effectiveDate=rows[0]["effectiveDate"],
            )
            rows.append(deletion)
    write_oplog(draft, rows)
    for message in report:
        click.echo(message)
    if failed:
        raise click.ClickException("unresolved rows; check them with `entry --name`")
