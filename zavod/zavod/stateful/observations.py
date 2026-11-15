from datetime import datetime

from nomenklatura.db import Session
from sqlalchemy import func, select

from zavod.stateful.model import observation_table


def observe(
    db: Session, dataset: str, key: str, checksum: str, seen: datetime
) -> datetime:
    """Record that a source record was seen, and return when it was first seen.

    Use `zavod.helpers.observe_item` and `zavod.helpers.observe_change` in
    crawlers; this takes `seen` explicitly so that a backfill can replay archived
    source snapshots at their own dates, oldest first.

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
        The time the key was first seen with this checksum.
    """
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
        return seen

    first_seen: datetime = row.first_seen
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
    return first_seen
