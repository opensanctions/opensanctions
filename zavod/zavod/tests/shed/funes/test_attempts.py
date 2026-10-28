"""Tests for funes due-candidate selection."""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from pravda.db import SnapshotRecord
from sqlalchemy import insert, select

from zavod.context import Context
from zavod.meta import Dataset as MetaDataset
from zavod.shed.funes.catalogue import Candidate, Dataset
from zavod.shed.funes.definitions import DatasetDefinition
from zavod.shed.funes.model import (
    InspectionOutcome,
    SnapshotStatus,
    attempt_table,
    candidate_table,
)
from zavod.tests.util import make_context


def definition(**overrides: Any) -> DatasetDefinition:
    data: dict[str, Any] = {
        "name": "testdataset1",
        "people_sought": "board members",
        "subject_label": "Organization",
        "subjects": [
            {
                "name": "Bank A",
                "urls": ["https://a.example/about", "https://a.example/board"],
            },
            {"name": "Bank B", "urls": ["https://b.example/board"]},
        ],
    }
    data.update(overrides)
    return DatasetDefinition.model_validate(data)


def candidate_id(context: Context, url: str) -> uuid.UUID:
    return context.db.execute(
        select(candidate_table.c.id).where(candidate_table.c.url == url)
    ).scalar_one()


def make_attempt(
    context: Context,
    candidate_id: uuid.UUID,
    created_at: datetime,
    *,
    attempt_id: uuid.UUID | None = None,
    status: SnapshotStatus,
    outcome: InspectionOutcome | None = None,
    reason: str | None = None,
    model: str | None = None,
) -> None:
    """The snapshot row satisfies the attempt's FK; ``created_at``
    overrides the server default so tests can age attempts."""
    snapshot_id = uuid.uuid4()
    context.db.execute(
        SnapshotRecord.__table__.insert().values(
            id=snapshot_id, url=f"https://example.com/{snapshot_id}"
        )
    )
    context.db.execute(
        insert(attempt_table).values(
            id=attempt_id or uuid.uuid4(),
            candidate_id=candidate_id,
            snapshot_id=snapshot_id,
            created_at=created_at,
            status=status,
            outcome=outcome,
            reason=reason,
            model=model,
        )
    )


def test_unattempted_candidates_due(testdataset1: MetaDataset, funes_db) -> None:
    context = make_context(testdataset1)
    Dataset.sync(context.db, definition())
    due = Candidate.select_due(context.db, context.dataset.name)
    assert [(c.subject_name, c.url) for c in due] == [
        ("Bank A", "https://a.example/about"),
        ("Bank A", "https://a.example/board"),
        ("Bank B", "https://b.example/board"),
    ]
    assert all(c.subject_id is not None for c in due)


def test_recent_hit_blocks(testdataset1: MetaDataset, funes_db) -> None:
    context = make_context(testdataset1)
    Dataset.sync(context.db, definition())
    make_attempt(
        context,
        candidate_id(context, "https://a.example/board"),
        datetime.now(UTC),
        status=SnapshotStatus.USABLE,
        outcome=InspectionOutcome.HIT,
        model="inspector-model",
    )
    assert [c.url for c in Candidate.select_due(context.db, context.dataset.name)] == [
        "https://a.example/about",
        "https://b.example/board",
    ]


def test_aged_hit_due_again(testdataset1: MetaDataset, funes_db) -> None:
    context = make_context(testdataset1)
    Dataset.sync(context.db, definition())
    make_attempt(
        context,
        candidate_id(context, "https://a.example/board"),
        datetime.now(UTC) - timedelta(days=60),
        status=SnapshotStatus.USABLE,
        outcome=InspectionOutcome.HIT,
        model="inspector-model",
    )
    assert [c.url for c in Candidate.select_due(context.db, context.dataset.name)] == [
        "https://a.example/about",
        "https://a.example/board",
        "https://b.example/board",
    ]


def test_miss_blocks_regardless_of_age(testdataset1: MetaDataset, funes_db) -> None:
    context = make_context(testdataset1)
    Dataset.sync(context.db, definition())
    make_attempt(
        context,
        candidate_id(context, "https://a.example/board"),
        datetime.now(UTC) - timedelta(days=60),
        status=SnapshotStatus.USABLE,
        outcome=InspectionOutcome.MISS,
        reason="nothing satisfies the brief",
        model="inspector-model",
    )
    assert [c.url for c in Candidate.select_due(context.db, context.dataset.name)] == [
        "https://a.example/about",
        "https://b.example/board",
    ]


def test_broken_blocks_regardless_of_age(testdataset1: MetaDataset, funes_db) -> None:
    context = make_context(testdataset1)
    Dataset.sync(context.db, definition())
    make_attempt(
        context,
        candidate_id(context, "https://a.example/board"),
        datetime.now(UTC) - timedelta(days=60),
        status=SnapshotStatus.BROKEN,
        reason="rejected by deterministic checks",
    )
    assert [c.url for c in Candidate.select_due(context.db, context.dataset.name)] == [
        "https://a.example/about",
        "https://b.example/board",
    ]


def test_latest_attempt_decides(testdataset1: MetaDataset, funes_db) -> None:
    context = make_context(testdataset1)
    Dataset.sync(context.db, definition())
    make_attempt(
        context,
        candidate_id(context, "https://a.example/board"),
        datetime.now(UTC) - timedelta(days=60),
        status=SnapshotStatus.USABLE,
        outcome=InspectionOutcome.HIT,
        model="inspector-model",
    )
    make_attempt(
        context,
        candidate_id(context, "https://a.example/board"),
        datetime.now(UTC),
        status=SnapshotStatus.USABLE,
        outcome=InspectionOutcome.MISS,
        reason="nothing satisfies the brief anymore",
        model="inspector-model",
    )
    assert [c.url for c in Candidate.select_due(context.db, context.dataset.name)] == [
        "https://a.example/about",
        "https://b.example/board",
    ]


def test_created_at_tie_broken_by_greater_id(
    testdataset1: MetaDataset, funes_db
) -> None:
    context = make_context(testdataset1)
    Dataset.sync(context.db, definition())
    aged = datetime.now(UTC) - timedelta(days=60)
    older, newer = sorted((uuid.uuid4(), uuid.uuid4()))
    make_attempt(
        context,
        candidate_id(context, "https://a.example/board"),
        aged,
        attempt_id=older,
        status=SnapshotStatus.USABLE,
        outcome=InspectionOutcome.HIT,
        model="inspector-model",
    )
    make_attempt(
        context,
        candidate_id(context, "https://a.example/board"),
        aged,
        attempt_id=newer,
        status=SnapshotStatus.USABLE,
        outcome=InspectionOutcome.MISS,
        reason="nothing satisfies the brief",
        model="inspector-model",
    )
    assert [c.url for c in Candidate.select_due(context.db, context.dataset.name)] == [
        "https://a.example/about",
        "https://b.example/board",
    ]


def test_scoped_to_context_dataset(testdataset1: MetaDataset, funes_db) -> None:
    context = make_context(testdataset1)
    Dataset.sync(context.db, definition())
    other = definition(
        name="other",
        subjects=[{"name": "Bank C", "urls": ["https://c.example/board"]}],
    )
    Dataset.sync(context.db, other)
    assert [c.url for c in Candidate.select_due(context.db, context.dataset.name)] == [
        "https://a.example/about",
        "https://a.example/board",
        "https://b.example/board",
    ]
