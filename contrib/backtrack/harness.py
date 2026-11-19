"""Shared plumbing for replaying a dataset's source history into observations.

Each per-dataset script iterates the versions downloaded by `backtrack.fetch`
oldest first and records observations at the time of each version, into a
scratch SQLite database at `<data path>/backtrack/<dataset>.sqlite3`, and then
dumps them as a PostgreSQL script to import into production. The first version
is the dataset's baseline: records already in it stay undated.

This is throw-away code for a one-off backfill. Per-dataset scripts copy the
key and checksum logic from their crawler rather than sharing it: duplicate the
crawler as extensively as needed and don't factor anything out of it. Once the
backfill is imported, the crawler is the only place that computes observations.
"""

from collections.abc import Iterator
from datetime import datetime
from pathlib import Path

from nomenklatura.db import Session, get_engine, make_session
from sqlalchemy import select
from zavod.stateful.model import observation_table
from zavod.stateful.observations import observe

from zavod import settings


def iter_versions(dataset: str, resource: str) -> Iterator[tuple[datetime, Path]]:
    """Yield the time and path of each downloaded version of a resource, oldest first."""
    src_dir = settings.DATA_PATH / "backtrack" / dataset
    paths = sorted(src_dir.glob(f"*-{resource}"))
    if len(paths) == 0:
        raise RuntimeError(
            f"No versions of {resource} in {src_dir}, run backtrack.fetch"
        )
    for path in paths:
        stamp, _ = path.name.split("-", 1)
        # Naive UTC, like settings.RUN_TIME and the observation columns.
        yield datetime.strptime(stamp, "%Y%m%d%H%M%S"), path  # noqa: DTZ007


def open_db(dataset: str) -> Session:
    """Open the dataset's scratch database, with its observations cleared."""
    path = settings.DATA_PATH / "backtrack" / f"{dataset}.sqlite3"
    url = f"sqlite:///{path.as_posix()}"
    observation_table.create(get_engine(url), checkfirst=True)
    db = make_session(url)
    db.execute(observation_table.delete().where(observation_table.c.dataset == dataset))
    return db


def observe_designation(
    db: Session, dataset: str, key: str, checksum: str, seen: datetime
) -> str | None:
    """Record a designation's key and content, as `h.finalize_sanction` does.

    Scripts call this rather than `observe()`, so that the backfill records the
    same rows as the crawler.

    Returns:
        "listed" if the key is new at `seen`, "changed" if only its content is,
        and None otherwise, including for records at the baseline.
    """
    item_seen = observe(db, dataset, key, "", seen)
    change_seen = observe(db, dataset, key, checksum, seen)
    if item_seen == seen:
        return "listed"
    if change_seen == seen:
        return "changed"
    return None


def _sql_literal(value: str | datetime) -> str:
    if isinstance(value, datetime):
        return f"'{value.isoformat(sep=' ')}'"
    if "\x00" in value:
        raise ValueError(f"NUL character in value: {value!r}")
    return "'" + value.replace("'", "''") + "'"


def dump_sql(db: Session, dataset: str, batch_size: int = 1000) -> Path:
    """Write the dataset's observations as a PostgreSQL script.

    The script replaces all of the dataset's observations in one transaction,
    so it can be re-run after a fresh replay. It uses plain statements only,
    to run with `psql -f`, in Cloud SQL Studio or via `gcloud sql import sql`.
    """
    table = observation_table
    stmt = select(
        table.c.dataset,
        table.c.key,
        table.c.checksum,
        table.c.first_seen,
        table.c.last_seen,
    )
    stmt = stmt.where(table.c.dataset == dataset)
    stmt = stmt.order_by(table.c.key, table.c.checksum)
    rows = db.execute(stmt).fetchall()
    path = settings.DATA_PATH / "backtrack" / f"{dataset}.observations.sql"
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("BEGIN;\n")
        fh.write(f"DELETE FROM observation WHERE dataset = {_sql_literal(dataset)};\n")
        for start in range(0, len(rows), batch_size):
            fh.write(
                "INSERT INTO observation "
                "(dataset, key, checksum, first_seen, last_seen) VALUES\n"
            )
            values = [
                "(" + ", ".join(_sql_literal(v) for v in row) + ")"
                for row in rows[start : start + batch_size]
            ]
            fh.write(",\n".join(values) + ";\n")
        fh.write("COMMIT;\n")
    return path
