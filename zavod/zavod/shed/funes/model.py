"""Schema for the funes web-inspection pipeline state store.

The tables record pipeline state only: what is configured (dataset,
subject, candidate) and the terminal verdict of each completed run
(attempt). Extracted people and positions are emitted as FollowTheMoney
entities rather than stored.

``attempt.snapshot_id`` references the ``snapshot`` table, which is owned
by zavod's Alembic chain but defined by the pravda package
(``pravda.db.SnapshotRecord``). Snapshots are one-to-one with attempts:
each run captures its own snapshot, and the capture timestamp lives on
the snapshot row.

Tables live on their own metadata (``funes_meta``), passed explicitly
into the Alembic ``target_metadata`` list and created explicitly in
tests via ``funes_meta.create_all``. They are prefixed ``funes_``
because the database hosts other projects' tables (including zavod's
own ``position``). Following the stateful tables, access is Core-level,
not ORM.
"""

import uuid
from enum import StrEnum

from sqlalchemy import (
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Index,
    MetaData,
    Table,
    Text,
    UniqueConstraint,
    Uuid,
    func,
)

from pravda.db import SnapshotRecord

funes_meta = MetaData()


class SnapshotStatus(StrEnum):
    """Terminal assessment of a captured Pravda snapshot.

    - usable: the snapshot is a usable source; the run records an
      inspection ``outcome`` (hit or miss).
    - broken: the snapshot is unusable; ``model`` may be null when
      deterministic checks rejected the snapshot before the model ran.
    """

    USABLE = "usable"
    BROKEN = "broken"


class InspectionOutcome(StrEnum):
    """Terminal judgement of a usable snapshot against an inspection brief.

    - hit: the brief is satisfied; extracted entities are emitted.
    - miss: nothing satisfies the brief; ``reason`` explains why.
    """

    HIT = "hit"
    MISS = "miss"


# One input dataset. ``people_sought`` names the class of position
# holders to find; ``subject_label`` names each subject's role in the
# inspection brief.
dataset_table = Table(
    "funes_dataset",
    funes_meta,
    Column("id", Uuid, primary_key=True, default=uuid.uuid4),
    Column("name", Text, nullable=False),
    Column("people_sought", Text, nullable=False),
    Column("subject_label", Text, nullable=False),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    ),
    UniqueConstraint("name"),
)


# One named entity scoping a dataset's people sought.
subject_table = Table(
    "funes_subject",
    funes_meta,
    Column("id", Uuid, primary_key=True, default=uuid.uuid4),
    Column(
        "dataset_id",
        Uuid,
        ForeignKey("funes_dataset.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("name", Text, nullable=False),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    ),
    UniqueConstraint("dataset_id", "name"),
)


# The subject ↔ url relation: one unit of work for the pipeline. The same
# URL may be a candidate for several subjects, so URL normalisation is
# load-bearing: nothing else deduplicates differently-written equal URLs.
# ``attempt_id`` and ``reason`` record discovery provenance: the run whose
# discovery selected this candidate, and the selecting model's reason.
candidate_table = Table(
    "funes_candidate",
    funes_meta,
    Column("id", Uuid, primary_key=True, default=uuid.uuid4),
    Column(
        "subject_id",
        Uuid,
        ForeignKey("funes_subject.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("url", Text, nullable=False),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    ),
    Column("attempt_id", Uuid, nullable=True),
    Column("reason", Text, nullable=True),
    UniqueConstraint("subject_id", "url"),
)


# One completed run: the candidate inspected, the snapshot it captured,
# and the run's terminal verdict. Infra failures (exceptions) write no
# row.
attempt_table = Table(
    "funes_attempt",
    funes_meta,
    # The id is caller-allocated: it doubles as the model run id and the
    # repair-routing id.
    Column("id", Uuid, primary_key=True),
    Column(
        "candidate_id",
        Uuid,
        ForeignKey("funes_candidate.id", ondelete="CASCADE"),
        nullable=False,
    ),
    Column(
        "snapshot_id",
        Uuid,
        ForeignKey(SnapshotRecord.__table__.c.id, ondelete="RESTRICT"),
        nullable=False,
    ),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    ),
    # ``status`` stores a ``SnapshotStatus`` value, ``outcome`` an
    # ``InspectionOutcome`` value. A null ``model`` marks rejection by
    # deterministic checks before the model ran.
    Column("status", Text, nullable=False),
    Column("outcome", Text, nullable=True),
    Column("reason", Text, nullable=True),
    Column("model", Text, nullable=True),
    UniqueConstraint("snapshot_id"),
    CheckConstraint("status IN ('usable', 'broken')"),
    CheckConstraint("outcome IS NOT NULL OR status = 'broken'"),
    CheckConstraint(
        "outcome IS NULL OR (status = 'usable' AND outcome IN ('hit', 'miss'))"
    ),
    CheckConstraint("reason IS NULL OR status = 'broken' OR outcome = 'miss'"),
    CheckConstraint(
        "reason IS NOT NULL OR (status = 'usable' AND outcome = 'hit')"
    ),
    CheckConstraint("model IS NOT NULL OR status = 'broken'"),
)

Index(
    "ix_funes_attempt_candidate_created",
    attempt_table.c.candidate_id,
    attempt_table.c.created_at,
)
