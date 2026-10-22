"""Schema for the funes web-inspection pipeline state store.

The tables record pipeline state only: what is configured (dataset,
subject, candidate) and the terminal verdict of each completed run
(attempt). Extracted people and positions are emitted as FollowTheMoney
entities rather than stored.

```snapshot`` is the persisted form of a Pravda ``Snapshot``
value: Pravda captures and returns values; the runner persists them.
Artifact fields are storage-relative paths. Snapshots are one-to-one
with attempts: each run captures its own snapshot, and the capture
timestamp lives on the snapshot row. ``attempt.snapshot_id`` references
it.

Tables live on their own metadata (``meta``), passed explicitly
into the Alembic ``target_metadata`` list and created explicitly in
tests via ``meta.create_all``. The pipeline's own tables are prefixed
``funes_`` because the database hosts other projects' tables (including
zavod's own ``position``).
Following the stateful tables, access is Core-level,
not ORM.
"""

import uuid
from enum import StrEnum

from sqlalchemy import (
    JSON,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    Table,
    Text,
    UniqueConstraint,
    Uuid,
    func,
)
from sqlalchemy.dialects import postgresql

meta = MetaData()


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
# URL may be a candidate for several subjects, so URL normalisation is
# load-bearing: nothing else deduplicates differently-written equal URLs.
# ``attempt_id`` and ``reason`` record discovery provenance: the run whose
# discovery selected this candidate, and the selecting model's reason.
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


# One captured web page: the persisted form of a Pravda ``Snapshot``
# value. The id and timestamp are minted at capture by Pravda, so
# inserts always pass them explicitly; no insert path can omit them.
snapshot_table = Table(
    "snapshot",
    meta,
    Column("id", Uuid, primary_key=True),
    Column("url", Text, nullable=False),
    Column("final_url", Text, nullable=True),
    Column(
        "captured_at",
        DateTime(timezone=True),
        nullable=False,
    ),
    Column("http_status", Integer, nullable=True),
    Column("error", Text, nullable=True),
    Column("rendered_html", Text, nullable=True),
    Column("screenshot", Text, nullable=True),
    Column(
        "http_archive",
        JSON().with_variant(postgresql.JSONB(), "postgresql"),
        nullable=True,
    ),
)


# One completed run: the candidate inspected, the snapshot it captured,
# and the run's terminal verdict. Infra failures (exceptions) write no
# row.
attempt_table = Table(
    "funes_attempt",
    meta,
    # The id is caller-allocated: minted at run start, it identifies the
    # run — as the model run id, transcript basename, and repair-routing
    # id — long before this terminal row exists.
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
        ForeignKey("snapshot.id", ondelete="RESTRICT"),
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
    CheckConstraint(
        "status IN ('usable', 'broken')", name="ck_funes_attempt_status_domain"
    ),
    CheckConstraint(
        "outcome IS NOT NULL OR status = 'broken'",
        name="ck_funes_attempt_outcome_required_if_usable",
    ),
    CheckConstraint(
        "outcome IS NULL OR (status = 'usable' AND outcome IN ('hit', 'miss'))",
        name="ck_funes_attempt_outcome_implies_usable",
    ),
    CheckConstraint(
        "reason IS NULL OR status = 'broken' OR outcome = 'miss'",
        name="ck_funes_attempt_hit_without_reason",
    ),
    CheckConstraint(
        "reason IS NOT NULL OR (status = 'usable' AND outcome = 'hit')",
        name="ck_funes_attempt_reason_required_unless_hit",
    ),
    CheckConstraint(
        "model IS NOT NULL OR status = 'broken'",
        name="ck_funes_attempt_model_required_unless_broken",
    ),
)

Index(
    "ix_funes_attempt_candidate_created",
    attempt_table.c.candidate_id,
    attempt_table.c.created_at,
)
