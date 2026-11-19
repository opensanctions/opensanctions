from datetime import datetime
from functools import lru_cache

from nomenklatura.db import Session
from sqlalchemy import func, select

from zavod.stateful.model import observation_table


@lru_cache(maxsize=100)
def _load_baseline(db: Session, dataset: str) -> datetime | None:
    table = observation_table
    stmt = select(table.c.first_seen)
    stmt = stmt.where(table.c.dataset == dataset)
    stmt = stmt.where(table.c.key == "")
    stmt = stmt.where(table.c.checksum == "")
    baseline: datetime | None = db.execute(stmt).scalar()
    return baseline


def _baseline(db: Session, dataset: str, seen: datetime) -> datetime:
    """Return the dataset's first observation time, recording it on first use."""
    baseline = _load_baseline(db, dataset)
    if baseline is None:
        istmt = observation_table.insert().values(
            dataset=dataset, key="", checksum="", first_seen=seen, last_seen=seen
        )
        db.execute(istmt)
        # Drop the cached miss, so the next lookup reads the marker.
        _load_baseline.cache_clear()
        return seen
    return baseline


def observe(
    db: Session, dataset: str, key: str, checksum: str, seen: datetime
) -> datetime | None:
    """Record that a source record was seen, and return when it was first seen.

    Use `zavod.helpers.observe_item` and `zavod.helpers.observe_change` in
    crawlers; this takes `seen` explicitly so that a backfill can replay archived
    source snapshots at their own dates, oldest first.

    The dataset's first observation sets its baseline. A record first seen at the
    baseline was already in the source when we started looking, so its date is
    unknown and this returns None.

    A checksum keeps its `first_seen` only while it is the key's current one. If
    another checksum of the key was seen after it, the content changed back
    (A → B → A) and `first_seen` restarts at `seen`, so a modification date
    derived from it never moves backwards.

    Args:
        db: The database session to write to.
        dataset: The name of the dataset that publishes the record.
        key: The authority-issued ID of the record.
        checksum: A checksum of the record's source content, or "" to observe
            only the key.
        seen: The time the record was seen.

    Returns:
        The time the key was first seen with this checksum, or None if that was
        at the dataset's baseline.
    """
    assert len(key) > 0, "Observation key must not be empty"
    baseline = _baseline(db, dataset, seen)
    table = observation_table
    stmt = select(table.c.id, table.c.first_seen, table.c.last_seen)
    stmt = stmt.where(table.c.dataset == dataset)
    stmt = stmt.where(table.c.key == key)
    stmt = stmt.where(table.c.checksum == checksum)
    row = db.execute(stmt).first()
    if row is None:
        istmt = table.insert().values(
            dataset=dataset,
            key=key,
            checksum=checksum,
            first_seen=seen,
            last_seen=seen,
        )
        db.execute(istmt)
        first_seen = seen
    else:
        first_seen = row.first_seen
        if checksum != "":
            others = select(func.max(table.c.last_seen))
            others = others.where(table.c.dataset == dataset)
            others = others.where(table.c.key == key)
            others = others.where(table.c.checksum != checksum)
            others = others.where(table.c.checksum != "")
            other_seen: datetime | None = db.execute(others).scalar()
            if other_seen is not None and other_seen > row.last_seen:
                first_seen = seen

        ustmt = table.update().where(table.c.id == row.id)
        ustmt = ustmt.values(first_seen=first_seen, last_seen=max(row.last_seen, seen))
        db.execute(ustmt)
    if first_seen <= baseline:
        return None
    return first_seen
