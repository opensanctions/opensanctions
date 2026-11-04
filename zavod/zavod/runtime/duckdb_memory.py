"""Warn before a DuckDB instance outgrows its memory limit.

DuckDB can drop the cached blocks of database and input files from memory and
read them back when needed. Everything else it holds - indexes, in-memory
tables, the state of running queries - it can at most spill to its temp
directory, and indexes not even that. An instance whose held memory approaches
its limit at a point where it has no query running will fail with an
out-of-memory error once a query needs room, so the check runs at points like
that, as the data grows run over run.

This can't catch a single query that needs more memory than the limit while
it runs; those fail outright rather than growing towards the limit.
"""

import re

import duckdb

from zavod.logs import get_logger

log = get_logger(__name__)

# Share of the memory limit held outside file caches above which to warn.
WARN_RATIO = 0.8

# Memory tags of blocks DuckDB can drop and re-read from their files.
FILE_CACHE_TAGS = ("BASE_TABLE", "EXTERNAL_FILE_CACHE")

UNITS = {
    "bytes": 1,
    "KiB": 2**10,
    "MiB": 2**20,
    "GiB": 2**30,
    "TiB": 2**40,
    "KB": 10**3,
    "MB": 10**6,
    "GB": 10**9,
    "TB": 10**12,
}
SIZE = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]+)\s*$")


def parse_size(text: str) -> int:
    """Parse a size as DuckDB renders settings, e.g. "2.3 GiB", into bytes.

    DuckDB rounds to one decimal, so the result is only approximate."""
    match = SIZE.match(text)
    if match is None or match.group(2) not in UNITS:
        raise ValueError(f"Unrecognised DuckDB size: {text!r}")
    return int(float(match.group(1)) * UNITS[match.group(2)])


def memory_held_ratio(conn: duckdb.DuckDBPyConnection) -> float:
    """The share of the instance's memory limit held outside file caches."""
    row = conn.execute("SELECT current_setting('memory_limit')").fetchone()
    assert row is not None
    limit = parse_size(str(row[0]))
    tags = ", ".join(f"'{tag}'" for tag in FILE_CACHE_TAGS)
    row = conn.execute(
        "SELECT coalesce(sum(memory_usage_bytes), 0) FROM duckdb_memory() "
        f"WHERE tag NOT IN ({tags})"
    ).fetchone()
    assert row is not None
    return int(row[0]) / limit


def warn_memory_pressure(conn: duckdb.DuckDBPyConnection, instance: str) -> None:
    """Log a warning if the instance holds more than WARN_RATIO of its limit
    outside file caches. Call it between queries."""
    try:
        ratio = memory_held_ratio(conn)
    except ValueError as exc:
        # A check that can't read the limit must not fail the run it guards.
        log.warning(
            f"Could not check the memory of DuckDB instance {instance!r}: {exc}",
            instance=instance,
        )
        return
    if ratio > WARN_RATIO:
        log.warning(
            f"DuckDB instance {instance!r} holds {ratio:.0%} of its memory limit. "
            "Raise its memory limit before it runs out of memory.",
            instance=instance,
            memory_held_ratio=round(ratio, 3),
        )
