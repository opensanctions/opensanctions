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

import re
from dataclasses import dataclass
from pathlib import Path

import click
from normality import ascii_text
from rigour.names import tokenize_name
from rigour.text.distance import levenshtein_similarity

from .common import (
    DEL_COLUMNS,
    OPLOG_COLUMNS,
    mint_id,
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
MONTHS = {
    m: i
    for i, m in enumerate(
        ("january", "february", "march", "april", "may", "june", "july")
        + ("august", "september", "october", "november", "december"),
        start=1,
    )
}
DOTTED_RE = re.compile(r"^(\d{1,2})\.(\d{1,2})\.(\d{4})$")
# Full month names ("2 December 1985") and UN abbreviations ("26 Feb. 2011").
WORDED_RE = re.compile(r"^(\d{1,2}) ([A-Za-z]+)(\.?) (\d{4})$")

TOKEN_MATCH = 0.7
LISTING_BONUS = 0.3
RECORD_BONUS = 0.15
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
        values = [row[column]] if column == "name" else split_multi(row[column])
        for value in values:
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


def parse_date(text: str) -> tuple[int, int, int] | None:
    """Read a full printed date; partial or other shapes give None."""
    dotted = DOTTED_RE.match(text.strip())
    if dotted is not None:
        day, month, year = dotted.groups()
        return (int(year), int(month), int(day))
    worded = WORDED_RE.match(text.strip())
    if worded is None:
        return None
    day, word, dot, year = worded.groups()
    word = word.lower()
    if dot:
        found = [n for m, n in MONTHS.items() if m.startswith(word) and len(word) >= 3]
        month = found[0] if len(found) == 1 else None
    else:
        month = MONTHS.get(word)
    return None if month is None else (int(year), month, int(day))


def dates(cell: str) -> set[tuple[int, int, int]]:
    found = {parse_date(value) for value in split_multi(cell)}
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


@cli.command()
@click.argument("framework")
@click.option("--before", required=True, help="The act doing the amending.")
@click.option("--name", required=True)
@click.option("--schema", default="Person")
@click.option("--program", default="EU-UKR")
@click.option("--measure", default="Asset freeze")
@click.option("--birth-date", default="")
@click.option("--listing-date", default="")
def find(
    framework: str,
    before: str,
    name: str,
    schema: str,
    program: str,
    measure: str,
    birth_date: str,
    listing_date: str,
) -> None:
    """Rank live designations against a printed name."""
    row = {column: "" for column in NAME_COLUMNS}
    row.update(
        name=name,
        schema=schema,
        programKey=program,
        measure=measure,
        birthDate=birth_date,
        listingDate=listing_date,
    )
    state = replay(framework, before=before)
    for match in rank(row, list(state.live.values()))[:5]:
        click.echo(describe(match))


@cli.command()
@click.argument("framework")
@click.argument("draft", type=click.Path(exists=True, path_type=Path))
@click.option("--pin", multiple=True, help="LINE=ID, for a match to set by hand.")
@click.option(
    "--delete-untouched",
    is_flag=True,
    help="The act replaces the whole list: append a DEL for every live entry it leaves out.",
)
def resolve(
    framework: str, draft: Path, pin: tuple[str, ...], delete_untouched: bool
) -> None:
    """Fill empty `id` cells of DRAFT in place.

    MOD and DEL rows take the id, annex and recordId of the live entry they
    match; ADD rows get a minted id. The draft's file stem names the act, so
    matching sees the state just before it.
    """
    pins = dict(p.split("=", 1) for p in pin)
    state = replay(framework, before=draft.stem)
    by_id = {entry.row["id"]: entry for entry in state.live.values()}
    rows = read_oplog(draft)
    claimed = {row["id"] for row in rows if row["id"] != ""}

    def settle(line: int, row: dict[str, str], entry: Entry, how: str) -> None:
        row["id"] = entry.row["id"]
        claimed.add(row["id"])
        for column in ("annex", "recordId"):
            if row[column] == "":
                row[column] = entry.row[column]
        click.echo(f"{line} {row['op']} {row['name']} -> {how}")

    pending: dict[int, dict[str, str]] = {}
    for line, row in enumerate(rows, start=2):
        if row["id"] != "":
            continue
        if row["op"] == "ADD":
            row["id"] = mint_id(framework, row["recordId"], row["name"])
            click.echo(f"{line} ADD minted {row['id'][:10]} {row['name']}")
        elif str(line) in pins:
            settle(line, row, by_id[pins[str(line)]], "pinned")
        else:
            pending[line] = row
    # An act amends each entry once: clear matches claim their entries first,
    # and the remaining rows are ranked against what is still unclaimed.
    progress = True
    while pending and progress:
        progress = False
        for line, row in list(pending.items()):
            free = [e for e in state.live.values() if e.row["id"] not in claimed]
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
        free = [e for e in state.live.values() if e.row["id"] not in claimed]
        click.echo(f"{line} {row['op']} UNRESOLVED {row['name']}")
        for match in rank(row, free)[:3]:
            click.echo(f"    {describe(match)}")
    # When an act replaces a whole annex, these are the entries it drops.
    touched = {row["id"] for row in rows}
    untouched = [e for e in state.live.values() if e.row["id"] not in touched]
    click.echo(f"{len(untouched)} live entries not touched by this act")
    for entry in untouched:
        click.echo(
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
    if failed:
        raise click.ClickException("unresolved rows; check them with `find`")


if __name__ == "__main__":
    cli()
