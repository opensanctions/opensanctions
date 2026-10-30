"""Normalize, checksum and group the issues emitted by a crawler run."""

import hashlib
import json
import re
from typing import Any

# Python object reprs carry a memory address (e.g. "<Element div at 0x7f...>")
# that changes every run. The address tells us nothing about which issue this is,
# so strip it before checksumming or grouping.
HEX_ADDR_RE = re.compile(r"0x[0-9a-fA-F]+")


def is_issue_skipped(issue: dict[str, Any]) -> bool:
    """True for issues a crawler logged with `agent="skip"`.

    Crawlers set the attribute on warnings that need a human outside this
    repository, such as the review UI backlog.
    """
    data = issue.get("data")
    return isinstance(data, dict) and data.get("agent") == "skip"


def issues_checksum(issues: list[Any]) -> str:
    """Return a checksum that identifies a constant set of crawl issues.

    Use this to seed a deterministic branch name: two crawls that surface the
    same warnings hash to the same value, so a re-run over an unchanged issue
    set reuses the branch (and dedupes against an existing PR), while a genuinely
    changed set produces a new branch.

    Only run-invariant content is hashed. Each issue's `timestamp` and `id` are
    dropped — `id` is itself a hash that folds in the timestamp, so it churns
    every run — and memory addresses are scrubbed from the remaining fields. The
    page-content hashes that live in `data` (e.g. the expected/actual values of a
    "DOM hash changed" warning) are stable and are kept, so a real source change
    still moves the checksum.

    Skipped issues are left out: their messages often carry a count that
    changes every run, e.g. the review backlog size, and would otherwise
    move the checksum while the actionable issues stay the same.
    """
    normalized = [
        {k: issue.get(k) for k in ("level", "module", "message", "entity", "data")}
        for issue in issues
        if not is_issue_skipped(issue)
    ]
    # Sort so the order issues happen to appear in the log doesn't affect the hash.
    normalized.sort(key=lambda i: json.dumps(i, sort_keys=True, ensure_ascii=False))
    canonical = json.dumps(normalized, sort_keys=True, ensure_ascii=False)
    canonical = HEX_ADDR_RE.sub("0x*", canonical)
    return hashlib.sha1(canonical.encode("utf-8")).hexdigest()


# Exact, case-sensitive markers of issues that cannot be addressed by editing
# this repository: transient infrastructure noise that resolves (or recurs) on
# its own, and backlogs handled in out-of-band systems.
#
# zavod's crawl.py logs RequestException failures as "Runner failed with
# {type}[ on {url}]" — only connection-level exception types are listed here.
# HTTPError is deliberately absent: a persistent 404/403 means a moved or
# bot-blocked source, which is actionable. All other exceptions are logged as
# "Runner failed: {str(exc)}", where a database deadlock surfaces the
# qualified driver exception name.
IGNORED_MESSAGES = [
    "Runner failed with ConnectionError",
    "Runner failed with ConnectTimeout",
    "Runner failed with ReadTimeout",
    "Runner failed with Timeout",
    "psycopg2.errors.DeadlockDetected",
    "psycopg.errors.DeadlockDetected",
    # zavod.stateful.review.assert_all_accepted — cleared by a human in the
    # review UI, which the agent has no access to. Occurs both as a warning
    # and, raised, as "Runner failed: There are N unaccepted items...".
    "unaccepted items for dataset",
]


def is_issue_ignored(issue: dict[str, Any]) -> bool:
    """True for issues that cannot be addressed by editing this repository.

    Use this to avoid spawning an agent for a run whose issues it could never
    fix: transient infrastructure noise (a database deadlock, a dropped
    connection, a timeout) and review-system backlog, which a human clears in
    the review UI. The match is deliberately narrow: issues a crawler logged
    with `agent="skip"`, plus exact message markers for known cases, so
    anything unrecognized stays visible.
    """
    if is_issue_skipped(issue):
        return True
    message = str(issue.get("message", ""))
    return any(pattern in message for pattern in IGNORED_MESSAGES)


# For grouping we normalize more aggressively than for checksumming: values
# that vary per occurrence (numbers, quoted/bracketed payloads) are collapsed
# so e.g. every "Assertion ... 379 is not >= 400" instance lands in one bucket.
_DIGITS_RE = re.compile(r"\d+")
_QUOTED_RE = re.compile(r"'[^']*'|\"[^\"]*\"")


def normalize_message(message: str) -> str:
    """Collapse an issue message to its run-invariant pattern for grouping."""
    message = HEX_ADDR_RE.sub("0x*", message)
    message = _QUOTED_RE.sub("'…'", message)
    message = _DIGITS_RE.sub("N", message)
    return message


def group_issues(
    issues: list[dict[str, Any]],
) -> list[tuple[str, list[dict[str, Any]]]]:
    """Group issues by normalized message pattern, largest group first.

    Returns (pattern, members) pairs; each members list keeps the original
    issue dicts so callers can show a representative example.
    """
    groups: dict[str, list[dict[str, Any]]] = {}
    for issue in issues:
        pattern = normalize_message(str(issue.get("message", "")))
        groups.setdefault(pattern, []).append(issue)
    return sorted(groups.items(), key=lambda kv: len(kv[1]), reverse=True)
