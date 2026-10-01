"""Schema for the funes web-inspection pipeline state store.

The tables record pipeline state only: what is configured (dataset,
subject, candidate), what was captured (attempt, snapshot assessment),
and what the inspection concluded (inspection). Extracted people and
positions are emitted as FollowTheMoney entities rather than stored.

``attempt.snapshot_id`` references the ``snapshot`` table, which is owned
by zavod's Alembic chain but defined by the pravda package
(``pravda.db.SnapshotRecord``). Snapshots are one-to-one with
attempts: each inspection run captures its own snapshot.

Tables live on zavod's shared metadata (``zavod.db.meta``) so that the
Alembic migration chain and the test-suite ``create_all`` pick them up.
They are prefixed ``funes_`` because the database hosts other projects'
tables (including zavod's own ``position``). Following the stateful
tables, access is Core-level (``zavod.shed.funes.model`` tables plus
``nomenklatura.db.Session``), not ORM.
"""

import uuid
from enum import StrEnum

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Index,
    Table,
    Text,
    UniqueConstraint,
    Uuid,
    func,
)

from pravda.db import SnapshotRecord
from zavod.db import meta


class SnapshotStatus(StrEnum):
    """Terminal assessment of a captured Pravda snapshot.

    - usable: the snapshot is a usable source; an Inspection follows.
    - broken: the snapshot is unusable. ``reason`` is required; ``model``
      is null only when deterministic checks rejected the snapshot before
      the model ran.
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


# One input dataset, named after its source YAML filename stem.
# ``people_sought`` names the class of position holders to find;
# ``subject_label`` names each subject's role in the inspection brief.
dataset_table = Table(
    "funes_dataset",
    meta,
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
    meta,
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
# URL may be a candidate for several subjects, so URL normalisation
# (``urldefrag`` in ``sources.py``) is load-bearing: nothing else
# deduplicates differently-written equal URLs. ``attempt_id`` and
# ``reason`` record discovery provenance for candidates found by the
# discovery agent rather than seeded from YAML.
candidate_table = Table(
    "funes_candidate",
    meta,
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


# One completed domain run connecting a candidate to a Pravda snapshot.
# The snapshot stays a logical UUID reference into Pravda's own storage;
# infra failures (exceptions) create no row.
attempt_table = Table(
    "funes_attempt",
    meta,
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
    Column("captured_at", DateTime(timezone=True), nullable=False),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    ),
    UniqueConstraint("snapshot_id"),
)

Index("ix_funes_attempt_candidate_created", attempt_table.c.candidate_id, attempt_table.c.created_at)


# Terminal assessment of one snapshot, keyed by its snapshot id. ``status``
# stores a ``SnapshotStatus`` value.
snapshot_assessment_table = Table(
    "funes_snapshot_assessment",
    meta,
    Column(
        "snapshot_id",
        Uuid,
        ForeignKey("funes_attempt.snapshot_id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column("status", Text, nullable=False),
    Column("reason", Text, nullable=True),
    Column("model", Text, nullable=True),
    Column(
        "assessed_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    ),
)


# The judgement of a usable snapshot against one inspection brief. A
# hit's extracted people and positions are emitted as FollowTheMoney
# entities by the inspection runner; no person or position rows are
# stored. ``outcome`` stores an ``InspectionOutcome`` value.
inspection_table = Table(
    "funes_inspection",
    meta,
    Column("id", Uuid, primary_key=True, default=uuid.uuid4),
    Column(
        "attempt_id",
        Uuid,
        ForeignKey("funes_attempt.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
    ),
    Column("outcome", Text, nullable=False),
    Column("reason", Text, nullable=True),
    Column("model", Text, nullable=False),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    ),
)
