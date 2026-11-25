"""Find the earlier designation an amending act's entry refers to.

Amending acts address existing entries by name, often in a different form
than the one listed: reordered ("Ozerov, Viktor" / "Viktor OZEROV"), with a
patronymic added, a title prefixed, or a different transliteration. The
scorer compares transliterated token sets fuzzily across every name-like
value on both sides, and weighs in the printed listing date (replacement
entries reprint it) and the date of birth.

`find` ranks candidates for one name; `resolve` fills the `id` cells of a
draft file in place — matched for MOD/DEL rows, minted for ADD rows.
"""

import csv
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path

import click
from normality import ascii_text
from rigour.names import tokenize_name
from rigour.text.distance import levenshtein_similarity

from .act import OUT_DIR
from .common import (
    DEL_COLUMNS,
    OPLOG_COLUMNS,
    cell_values,
    mint_id,
    parse_date,
    read_oplog,
    split_multi,
    write_oplog,
)
from .replay import Entry, replay

NAME_COLUMNS = ("name", "alias", "weakAlias", "previousName")
PAREN_RE = re.compile(r"\(([^()]*)\)")
AKA_RE = re.compile(r"\b(?:a\.k\.a\.?|f\.k\.a\.?|alias)\s", re.IGNORECASE)
# Quotes often abut words without a space (‘Crimea’‘Chernomorneftegaz’).
QUOTES_RE = re.compile(r"[‘’“”'\"«»]")
TOKEN_MATCH = 0.7
LISTING_BONUS = 0.3
RECORD_BONUS = 0.15
ANNEX_BONUS = 0.1
PRIMARY_BONUS = 0.2
# A match through an alias ranks below the same match on the primary names:
# UN-style lists reuse one person's alias as another's name.
ALIAS_WEIGHT = 0.9
BIRTH_BONUS = 0.2
BIRTH_CONFLICT = -0.2
ACCEPT = 0.9
CONTAINED = 0.7
MARGIN = 0.25


def name_forms(value: str) -> list[list[str]]:
    """Token lists for a printed name and each parenthesised or a.k.a. part."""
    parts = [PAREN_RE.sub(" ", value), *PAREN_RE.findall(value)]
    forms: list[list[str]] = []
    for part in parts:
        for segment in AKA_RE.split(part):
            text = ascii_text(QUOTES_RE.sub(" ", segment)) or ""
            tokens = [t for t in tokenize_name(text.casefold()) if len(t) > 1]
            if tokens:
                forms.append(tokens)
    return forms


def row_forms(row: dict[str, str]) -> list[list[str]]:
    forms: list[list[str]] = []
    for column in NAME_COLUMNS:
        for value in cell_values(row, column):
            forms.extend(name_forms(value))
    return forms


def token_overlap(left: list[str], right: list[str]) -> float:
    """Dice-style overlap of two token lists, with fuzzy token matches.

    A name extended by more parts ("Taha" / "Ahmad Taha Khalid Abdul Qadir")
    keeps a floor score when every token of the shorter one is matched.
    """
    unused = list(right)
    matched, count = 0.0, 0
    for token in left:
        best, best_sim = None, 0.0
        for other in unused:
            sim = levenshtein_similarity(token, other)
            if sim > best_sim:
                best, best_sim = other, sim
        if best is not None and best_sim >= TOKEN_MATCH:
            unused.remove(best)
            matched += best_sim
            count += 1
    dice = 2 * matched / (len(left) + len(right))
    if count == min(len(left), len(right)):
        return max(dice, CONTAINED)
    return dice


def name_score(query: list[list[str]], candidate: list[list[str]]) -> float:
    return max((token_overlap(q, c) for q in query for c in candidate), default=0.0)


def dates(cell: str) -> set[tuple[int, int, int]]:
    found = {parse_date(value) for value in split_multi(cell) if value}
    return {d for d in found if d is not None}


@dataclass
class Match:
    entry: Entry
    score: float
    evidence: str


def rank(row: dict[str, str], entries: list[Entry]) -> list[Match]:
    """Score live entries of the same schema, program and measure against a row."""
    query = row_forms(row)
    primary = name_forms(row["name"])
    births = dates(row["birthDate"])
    listing = parse_date(row["listingDate"])
    matches: list[Match] = []
    for entry in entries:
        other = entry.row
        if (other["schema"], other["programKey"], other["measure"]) != (
            row["schema"],
            row["programKey"],
            row["measure"],
        ):
            continue
        score = max(
            name_score(primary, name_forms(other["name"])),
            ALIAS_WEIGHT * name_score(query, row_forms(other)),
        )
        evidence = [f"name {score:.2f}"]
        # The act names the entry exactly as it is listed.
        if any(
            sorted(q) == sorted(c) for q in primary for c in name_forms(other["name"])
        ):
            score += PRIMARY_BONUS
            evidence.append("primary")
        if row["annex"] != "" and row["annex"] == other["annex"]:
            score += ANNEX_BONUS
            evidence.append("annex")
        if row["recordId"] != "" and row["recordId"] == other["recordId"]:
            score += RECORD_BONUS
            evidence.append("recordId")
        if listing is not None and listing == parse_date(other["listingDate"]):
            score += LISTING_BONUS
            evidence.append("listingDate")
        other_births = dates(other["birthDate"])
        if births and other_births:
            if births & other_births:
                score += BIRTH_BONUS
                evidence.append("birthDate")
            else:
                score += BIRTH_CONFLICT
                evidence.append("birthDate conflict")
        matches.append(Match(entry, score, ", ".join(evidence)))
    matches.sort(key=lambda m: m.score, reverse=True)
    return matches


def decide(matches: list[Match]) -> Match | None:
    """The top match if it is strong and clearly ahead of the runner-up."""
    if not matches or matches[0].score < ACCEPT:
        return None
    if len(matches) > 1 and matches[0].score - matches[1].score < MARGIN:
        return None
    return matches[0]


def describe(match: Match) -> str:
    row = match.entry.row
    return (
        f"{match.score:.2f} {row['id'][:10]} {row['annex']}#{row['recordId']} "
        f"{row['name']} [{match.evidence}]"
    )


@click.group()
def cli() -> None:
    pass


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


@cli.command()
@click.argument("framework")
@click.option("--before", required=True, help="The act doing the amending.")
@click.option("--name", required=True)
@click.option("--schema", default="Person")
@click.option("--program", default=None, help="Default: the stack's only programKey.")
@click.option("--measure", default=None, help="Default: the stack's only measure.")
@click.option("--annex", default="", help="Only entries in this annex.")
@click.option("--birth-date", default="")
@click.option("--listing-date", default="")
def find(
    framework: str,
    before: str,
    name: str,
    schema: str,
    program: str | None,
    measure: str | None,
    annex: str,
    birth_date: str,
    listing_date: str,
) -> None:
    """Rank live designations against a printed name."""
    entries = list(replay(framework, before=before).live.values())
    row = dict.fromkeys(OPLOG_COLUMNS, "")
    row.update(
        name=name,
        schema=schema,
        programKey=only(entries, "programKey", program),
        measure=only(entries, "measure", measure),
        annex=annex,
        birthDate=birth_date,
        listingDate=listing_date,
    )
    for match in rank(row, entries)[:5]:
        click.echo(describe(match))


@cli.command("row")
@click.argument("framework")
@click.argument("id", required=False)
@click.option("--before", default=None, help="The act doing the amending.")
@click.option("--annex", default=None, help="With --record, instead of an id.")
@click.option("--record", default=None, help="The entry's recordId in --annex.")
@click.option(
    "--as-mod",
    "as_mod",
    default=None,
    help="An act CELEX: set op=MOD and the act's dates from tools/out/<celex>/meta.json.",
)
@click.option(
    "--out",
    type=click.Path(path_type=Path),
    default=None,
    help="Write a complete op-log file here instead of printing.",
)
def show_row(
    framework: str,
    id: str | None,
    before: str | None,
    annex: str | None,
    record: str | None,
    as_mod: str | None,
    out: Path | None,
) -> None:
    """Print a live designation's current row, as the base for a full MOD.

    Name the entry by (a prefix of) its id, or by --annex and --record as a
    corrigendum or amending act addresses it. With --as-mod, the row comes
    out as a MOD dated like that act; change only what the act changes.
    """
    state = replay(framework, before=before or as_mod)
    if id is not None:
        found = [e for e in state.live.values() if e.row["id"].startswith(id)]
    elif annex is not None and record is not None:
        found = [
            e
            for e in state.live.values()
            if e.row["annex"] == annex and e.row["recordId"] == record
        ]
    else:
        raise click.UsageError("give an id, or --annex and --record")
    if not found:
        raise click.ClickException("no live designation matches")
    rows = [dict(entry.row) for entry in found]
    if as_mod is not None:
        meta_path = OUT_DIR / as_mod / "meta.json"
        if not meta_path.exists():
            raise click.UsageError(f"run `python -m tools.act {as_mod}` first")
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


@cli.command()
@click.argument("framework")
@click.argument("draft", type=click.Path(exists=True, path_type=Path))
@click.option("--pin", multiple=True, help="LINE=ID, for a match to set by hand.")
@click.option(
    "--delete-untouched",
    is_flag=True,
    help="The act replaces a whole list: append a DEL for every live entry it "
    "leaves out. Scope it with --annex when the act replaces one annex.",
)
@click.option("--show-untouched", is_flag=True, help="List the live entries left out.")
@click.option("--annex", "annexes", multiple=True, help="Limit the untouched entries.")
def resolve(
    framework: str,
    draft: Path,
    pin: tuple[str, ...],
    delete_untouched: bool,
    show_untouched: bool,
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
    claimed = {(row["id"], row["annex"]) for row in rows if row["id"] != ""}
    report: list[str] = []

    def settle(line: int, row: dict[str, str], entry: Entry, how: str) -> None:
        row["id"] = entry.row["id"]
        row["annex"] = entry.row["annex"]
        row["recordId"] = entry.row["recordId"]
        claimed.add((row["id"], row["annex"]))
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
            free = [e for e in live if (e.row["id"], e.row["annex"]) not in claimed]
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
        free = [e for e in live if (e.row["id"], e.row["annex"]) not in claimed]
        report.append(f"{line} {row['op']} UNRESOLVED {row['name']}")
        report.extend(f"    {describe(match)}" for match in rank(row, free)[:3])
    # When an act replaces a whole annex, these are the entries it drops.
    touched = {(row["id"], row["annex"]) for row in rows}
    untouched = [
        e
        for e in live
        if (e.row["id"], e.row["annex"]) not in touched
        and (not annexes or e.row["annex"] in annexes)
    ]
    report.append(f"{len(untouched)} live entries not touched by this act")
    for entry in untouched:
        if show_untouched or delete_untouched:
            report.append(
                f"    {entry.row['id'][:10]} {entry.row['annex']}#{entry.row['recordId']}"
                f" {entry.row['name']}"
            )
        if delete_untouched and not failed and rows:
            deletion = dict.fromkeys(OPLOG_COLUMNS, "")
            deletion.update({c: entry.row[c] for c in DEL_COLUMNS})
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
        raise click.ClickException("unresolved rows; check them with `find`")


if __name__ == "__main__":
    cli()
