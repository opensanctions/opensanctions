"""Check op-log stacks: against the contract, their acts, and consolidations.

`check` validates a framework's stack — the contract and the replay — and
checks each file whose act has been fetched against the act's text: every
transcribed value must be printed there, except values a MOD or DEL carries
over from its target, and the dates must agree with CELLAR. Errors fail the
run; warnings flag rows to look at again. `compare` checks a replay against
a EUR-Lex consolidation's text.
"""

import json
import re
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from typing import get_args

import click
from followthemoney import model
from zavod.shed.ojeu.cellar import cli_client
from zavod.stateful.programs import Measure, get_program_by_key

from .act import OUT_DIR, fetch_act
from .common import (
    DEL_COLUMNS,
    ENTITY_COLUMNS,
    MULTI_COLUMNS,
    OPLOG_COLUMNS,
    OPS,
    REGULATIONS_DIR,
    cell_values,
    parse_date,
    read_oplog,
    regime_files,
    related_acts,
    split_multi,
)
from .match import haystack, loosen, missing_values, name_score, row_forms
from .replay import Op, State, apply, load_ops, replay

ID_RE = re.compile(r"^[0-9a-f]{40}$")
ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
ISO_PARTIAL_RE = re.compile(r"^\d{4}(?:-\d{2}(?:-\d{2})?)?$")
# A printed label ("Name:", "Title:") left inside a name value.
LEAKED_LABEL_RE = re.compile(r"(?:^|\s)[A-Za-z][\w ().']{0,40}:(?:\s|$)")
# Alias labels and enumeration markers say a name cell holds several names.
ALIAS_MARKER_RE = re.compile(
    r"\b(?:a\.\s?k\.\s?a\.?|f\.\s?k\.\s?a\.?|alias)(?:\s|:)|(?:^|\s)\(?[a-e]\)\s?\S",
    re.IGNORECASE,
)
NAME_COLUMNS = ("name", "alias", "weakAlias", "previousName")
DATE_LIKE_RE = re.compile(r"\d{1,2}\.\d{1,2}\.\d{4}")
REQUIRED = ("name", "schema", "programKey", "measure")
MEASURES = frozenset(get_args(Measure))
WRONG_TARGET = 0.5


@dataclass
class Issue:
    level: str
    where: str
    message: str


def iso(value: str) -> date | None:
    if ISO_RE.match(value) is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def check_row(op: Op, issues: list[Issue]) -> None:
    """Contract and plausibility checks that need only the row itself."""
    row, where = op.row, op.where

    def error(message: str) -> None:
        issues.append(Issue("error", where, message))

    def warn(message: str) -> None:
        issues.append(Issue("warning", where, message))

    if row["op"] not in OPS:
        error(f"unknown op {row['op']!r}")
        return
    if ID_RE.match(row["id"]) is None:
        error(f"id must be 40 lowercase hex characters: {row['id']!r}")
    published, effective = iso(row["publishedDate"]), iso(row["effectiveDate"])
    if published is None or effective is None:
        error("publishedDate and effectiveDate must be ISO dates")
    elif abs((effective - published).days) > 1:
        warn(
            f"effectiveDate {effective} is not within a day of publication; check the final article"
        )
    for column in ("listingDate", "endDate"):
        value = row[column]
        if value != "" and not (ISO_PARTIAL_RE.match(value) or parse_date(value)):
            error(f"{column} has an unknown shape: {value!r}")
    if row["op"] == "DEL":
        filled = [c for c in OPLOG_COLUMNS if row[c] != "" and c not in DEL_COLUMNS]
        if filled:
            error(f"DEL rows fill only the key and cross-check columns: {filled}")
        return
    for column in REQUIRED:
        if row[column] == "":
            error(f"{column} is required")
    schema = model.get(row["schema"])
    if schema is None:
        error(f"unknown schema {row['schema']!r}")
    program = get_program_by_key(row["programKey"]) if row["programKey"] else None
    if row["programKey"] and program is None:
        error(f"unknown programKey {row['programKey']!r}")
    if row["measure"] not in MEASURES:
        error(f"unknown measure {row['measure']!r}")
    elif program is not None and row["measure"] not in program.measures:
        error(f"{row['programKey']} does not declare {row['measure']!r}")
    for column in ENTITY_COLUMNS:
        if row[column] != "" and schema is not None and column not in schema.properties:
            error(f"{column} is not a property of {schema.name}")
    for column in MULTI_COLUMNS:
        if row[column] == "":
            continue
        try:
            values = split_multi(row[column])
        except ValueError as exc:
            error(f"{column}: {exc}")
            continue
        if "" in values or len(set(values)) != len(values):
            error(f"{column} has empty or repeated values")
    for column in NAME_COLUMNS:
        for value in cell_values(row, column):
            if LEAKED_LABEL_RE.search(value) or ALIAS_MARKER_RE.search(value):
                warn(f"{column} holds a printed label or enumeration: {value[:60]!r}")
    if DATE_LIKE_RE.search(row["birthPlace"]):
        warn(f"birthPlace holds a date: {row['birthPlace']!r}")


def check_file(celex: str, ops: list[Op], issues: list[Issue]) -> None:
    """Checks across the rows of one act's file."""
    if len({op.row["publishedDate"] for op in ops}) > 1:
        issues.append(Issue("error", celex, "publishedDate differs between rows"))
    seen: set[tuple[str, str, str]] = set()
    rows: set[tuple[str, ...]] = set()
    for op in ops:
        if op.key in seen:
            issues.append(
                Issue(
                    "error",
                    op.where,
                    f"id {op.row['id'][:10]} twice in one act for "
                    f"{op.row['programKey']} / {op.row['measure']}",
                )
            )
        seen.add(op.key)
        values = tuple(op.row.values())
        if values in rows:
            issues.append(Issue("error", op.where, "duplicate row"))
        rows.add(values)


def check_op_against_state(op: Op, state: State, issues: list[Issue]) -> None:
    """Plausibility checks of a MOD against the entry it replaces."""
    row = op.row
    entry = state.live.get(op.key)
    if entry is None or row["op"] != "MOD":
        return
    before = entry.row
    if row["annex"] != before["annex"]:
        issues.append(
            Issue(
                "warning",
                op.where,
                f"MOD moves the entry from annex {before['annex']!r} to "
                f"{row['annex']!r}; check that the act restructures or transfers",
            )
        )
    if row["listingDate"] != before["listingDate"]:
        issues.append(
            Issue(
                "warning",
                op.where,
                f"MOD changes listingDate {before['listingDate']} -> {row['listingDate']}",
            )
        )
    score = name_score(row_forms(row), row_forms(before))
    if score < WRONG_TARGET:
        issues.append(
            Issue(
                "warning",
                op.where,
                f"MOD name {row['name']!r} is far from {before['name']!r} ({score:.2f}); wrong target?",
            )
        )


def check_acts(framework: str, present: set[str], issues: list[Issue]) -> None:
    """Compare the stack's files with the acts CELLAR relates to the framework."""
    acts = {act.celex: act.document_date for act in related_acts(framework)}
    for celex in sorted(present):
        # A corrigendum ("32014R0269R(01)") belongs to the act it corrects.
        base = celex.split("R(")[0]
        if base != framework and base not in acts:
            issues.append(
                Issue("error", celex, "CELLAR relates no such act to the framework")
            )
    newest = max((acts[c] for c in present if c in acts), default=None)
    for celex, adopted in sorted(acts.items(), key=lambda item: item[1]):
        if newest is not None and adopted <= newest and celex not in present:
            issues.append(
                Issue("warning", framework, f"no file for {celex} ({adopted})")
            )


def validate_framework(framework: str, online: bool = False) -> list[Issue]:
    issues: list[Issue] = []
    try:
        files = load_ops(framework)
    except ValueError as exc:
        return [Issue("error", framework, str(exc))]
    blank = [op.where for _, ops in files for op in ops if op.row["id"] == ""]
    if blank:
        # Replaying rows without ids only produces confusing follow-on errors.
        return [
            Issue("error", where, "empty id; run `python -m tools resolve`")
            for where in blank
        ]
    state = State()
    slots: dict[tuple[str, str], dict[str, str]] = {}
    for celex, ops in files:
        check_file(celex, ops, issues)
        for op in ops:
            check_row(op, issues)
            slot = slots.setdefault(
                (op.row["effectiveDate"], op.row["publishedDate"]), {}
            )
            other = slot.get(op.row["id"])
            if other is not None and other != celex:
                issues.append(
                    Issue(
                        "error",
                        op.where,
                        f"{other} touches the same id on the same dates; order undefined",
                    )
                )
            slot[op.row["id"]] = celex
            check_op_against_state(op, state, issues)
            errors = len(state.errors)
            apply(state, op)
            issues.extend(
                Issue("error", op.where, e.split(": ", 1)[1])
                for e in state.errors[errors:]
            )
    if online:
        check_acts(framework, {celex for celex, _ in files}, issues)
    return issues


SAME_DAY_RE = re.compile(
    r"enter into force on the (?:date|day) of its publication", re.IGNORECASE
)
NEXT_DAY_RE = re.compile(
    r"enter into force on the (?:day|date) following (?:that|the date) of its "
    r"publication",
    re.IGNORECASE,
)


def act_in_force(text: str, published: str) -> str | None:
    """The entry-into-force date an act's standard final article states."""
    if SAME_DAY_RE.search(text):
        return published
    if NEXT_DAY_RE.search(text):
        return (date.fromisoformat(published) + timedelta(days=1)).isoformat()
    return None


def check_against_act(framework: str, celex: str, issues: list[Issue]) -> None:
    """Check one file against the act it transcribes, if the act is fetched."""
    path = REGULATIONS_DIR / framework / f"{celex}.csv"
    directory = OUT_DIR / celex
    if not (directory / "text.txt").exists():
        return
    meta = json.loads((directory / "meta.json").read_text())
    text = haystack((directory / "text.txt").read_text(encoding="utf-8"))
    # A MOD or DEL reproduces its target's row; values carried over from the
    # target need not be printed in this act.
    targets = replay(framework, before=celex).live
    # CELLAR records no entry into force for corrigenda; they take effect on
    # publication.
    in_force = meta["entry_into_force"] or [meta["publication_date"]]
    for line, row in enumerate(read_oplog(path), start=2):
        where = f"{celex}:{line}"
        target = targets.get((row["id"], row["programKey"], row["measure"]))
        carried = target.row if target is not None and row["op"] != "ADD" else None
        if row["publishedDate"] != meta["publication_date"]:
            issues.append(
                Issue(
                    "error",
                    where,
                    f"publishedDate {row['publishedDate']} is not CELLAR's "
                    f"{meta['publication_date']}",
                )
            )
        if row["effectiveDate"] not in in_force:
            stated = act_in_force(text, meta["publication_date"])
            agrees = "agrees" if stated == row["effectiveDate"] else "disagrees"
            issues.append(
                Issue(
                    "warning",
                    where,
                    f"effectiveDate {row['effectiveDate']} is not CELLAR's {in_force}; "
                    f"the act's final article says {stated} ({agrees})",
                )
            )
        for column, value in missing_values(row, text, carried=carried):
            issues.append(
                Issue("error", where, f"{column} not in the act: {value[:80]!r}")
            )


CONSOLIDATION_RE = re.compile(r"^0\d{4}[A-Z]\d{4}-(\d{4})(\d{2})(\d{2})$")
# Header lines of a EUR-Lex consolidation: "►M2 | COUNCIL IMPLEMENTING
# REGULATION (EU) 2017/1568 of …" and "►C1 | Corrigendum, OJ L 251, …
# (2017/1568)".
AMENDED_RE = re.compile(
    r"^►?M\d+ \| .*?(REGULATION|DECISION)\b[^|]*?\([A-Za-z]+\) (?:No )?(\d+)/(\d+)",
    re.IGNORECASE,
)
# Acts whose text is wholly superseded are listed without the "►".
CORRECTED_RE = re.compile(r"^►?C\d+ \| Corrigendum.*\((?:No )?(\d+)/(\d+)\)")


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
    later: dict[str, int] = {}
    if any(
        sum(1 for stem in files if stem.startswith(f"{c}R(")) != n
        for c, n in corrected.items()
    ):
        # EUR-Lex folds corrigenda published after a consolidation's date
        # into it; those cannot be in a stack replayed to that date.
        for act in related_acts(framework):
            if act.relation == "corrects" and act.document_date > as_of:
                later[act.framework_celex] = later.get(act.framework_celex, 0) + 1
    for celex, count in sorted(corrected.items()):
        have = sum(1 for stem in files if stem.startswith(f"{celex}R("))
        if have == count:
            continue
        message = (
            f"{consolidation} cites {count} corrigenda of {celex}; the stack has {have}"
        )
        if have + later.get(celex, 0) >= count:
            click.echo(f"WARNING  {message}, the rest published after {as_of}")
            continue
        errors += 1
        click.echo(f"ERROR    {message}")
    parts: dict[str, int] = {}
    for entry in state.live.values():
        parts[entry.row["annex"]] = parts.get(entry.row["annex"], 0) + 1
    click.echo(
        "live entries per annex part (check against the printed entries): "
        + ", ".join(f"{annex or '-'} {n}" for annex, n in sorted(parts.items()))
    )
    click.echo(
        f"{consolidation}: {len(state.live)} live entries as of {as_of}, "
        f"{len(state.lapsed)} lapsed, {errors} errors"
    )
    return errors


@click.command("check")
@click.argument("frameworks", nargs=-1)
@click.option(
    "--online", is_flag=True, help="Also check the files against CELLAR's act list."
)
@click.option(
    "--fetch",
    is_flag=True,
    help="Fetch acts not yet in tools/out/ so every file is checked against its act.",
)
def check_command(frameworks: tuple[str, ...], online: bool, fetch: bool) -> None:
    """Check the op-log stacks of FRAMEWORKS (default: all of them).

    Files whose act is in tools/out/ (see `act`) are also checked against
    the act's text; `--fetch` fetches the missing ones first.
    """
    names = frameworks or tuple(
        sorted(p.name for p in REGULATIONS_DIR.iterdir() if p.is_dir())
    )
    failed = False
    for framework in names:
        issues = validate_framework(framework, online=online)
        if not any(i.level == "error" for i in issues):
            for path in regime_files(framework):
                if fetch and not (OUT_DIR / path.stem / "text.txt").exists():
                    with cli_client() as client:
                        fetch_act(client, path.stem, OUT_DIR / path.stem)
                check_against_act(framework, path.stem, issues)
        for issue in issues:
            click.echo(f"{issue.level.upper():8} {issue.where}: {issue.message}")
        errors = sum(1 for i in issues if i.level == "error")
        click.echo(f"{framework}: {errors} errors, {len(issues) - errors} warnings")
        failed = failed or errors > 0
    sys.exit(1 if failed else 0)


@click.command("compare")
@click.argument("framework")
@click.argument("consolidation")
def compare_command(framework: str, consolidation: str) -> None:
    """Check FRAMEWORK's replay against a EUR-Lex CONSOLIDATION's text.

    CONSOLIDATION is a consolidated CELEX such as 02014R0269-20140512. Entries
    the consolidation prints but the replay lacks are not detected: count
    them per annex part against the live counts this prints.
    """
    sys.exit(1 if compare(framework, consolidation) else 0)
