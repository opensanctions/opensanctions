"""Check op-log stacks for contract errors and implausible transcriptions.

Errors break the contract or the replay and fail the run. Warnings flag
rows a reviewer should look at again — a structural check cannot know the
act, but it can notice when a transcription looks unlike one.
"""

import re
import sys
from dataclasses import dataclass
from datetime import date
from typing import get_args

import click
from followthemoney import model
from zavod.stateful.programs import Measure, get_program_by_key

from .common import (
    DEL_COLUMNS,
    ENTITY_COLUMNS,
    MULTI_COLUMNS,
    OPLOG_COLUMNS,
    OPS,
    REGULATIONS_DIR,
    cell_values,
    parse_date,
    related_acts,
    split_multi,
)
from .lookup import name_score, row_forms
from .replay import Op, State, apply, load_ops

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
    seen: set[tuple[str, str, str, str]] = set()
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
            Issue("error", where, "empty id; run `python -m tools.lookup resolve`")
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


@click.command()
@click.argument("frameworks", nargs=-1)
@click.option(
    "--online", is_flag=True, help="Also check the files against CELLAR's act list."
)
def main(frameworks: tuple[str, ...], online: bool) -> None:
    """Validate the op-log stacks of FRAMEWORKS (default: all of them)."""
    names = frameworks or tuple(
        sorted(p.name for p in REGULATIONS_DIR.iterdir() if p.is_dir())
    )
    failed = False
    for framework in names:
        issues = validate_framework(framework, online=online)
        for issue in issues:
            click.echo(f"{issue.level.upper():8} {issue.where}: {issue.message}")
        errors = sum(1 for i in issues if i.level == "error")
        click.echo(f"{framework}: {errors} errors, {len(issues) - errors} warnings")
        failed = failed or errors > 0
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
