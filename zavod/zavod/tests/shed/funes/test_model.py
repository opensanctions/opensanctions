"""Tests for the funes state-store schema: the terminal-verdict state
machine enforced by the check constraints on ``funes_attempt``, and the
one-to-one relation between attempts and snapshots."""

import uuid

import pytest
from nomenklatura.db import Session
from pravda.db import Base as pravda_base, SnapshotRecord
from sqlalchemy import insert
from sqlalchemy.exc import IntegrityError

from zavod.shed.funes.model import (
    attempt_table,
    candidate_table,
    dataset_table,
    funes_meta,
    InspectionOutcome,
    SnapshotStatus,
    subject_table,
)


@pytest.fixture(scope="function")
def funes_db(session: Session) -> Session:
    """The database session with the funes tables created. The pravda-owned
    ``snapshot`` table is created first: ``funes_attempt`` references it
    across metadata boundaries."""
    pravda_base.metadata.create_all(bind=session.connection)
    funes_meta.create_all(bind=session.connection)
    return session


def make_candidate(session: Session) -> tuple[uuid.UUID, uuid.UUID]:
    """Insert one dataset/subject/candidate/snapshot chain and return the
    candidate and snapshot ids."""
    dataset_id = uuid.uuid4()
    subject_id = uuid.uuid4()
    candidate_id = uuid.uuid4()
    snapshot_id = uuid.uuid4()
    session.execute(
        insert(dataset_table).values(
            id=dataset_id,
            name=f"dataset-{dataset_id}",
            people_sought="board members",
            subject_label="company",
        )
    )
    session.execute(
        insert(subject_table).values(
            id=subject_id,
            dataset_id=dataset_id,
            name=f"subject-{subject_id}",
        )
    )
    session.execute(
        insert(candidate_table).values(
            id=candidate_id,
            subject_id=subject_id,
            url=f"https://example.com/{candidate_id}",
        )
    )
    session.execute(
        insert(SnapshotRecord.__table__).values(
            id=snapshot_id,
            url=f"https://example.com/{candidate_id}",
        )
    )
    return candidate_id, snapshot_id


@pytest.mark.parametrize(
    ("status", "outcome", "reason", "model", "accepted"),
    [
        pytest.param(
            SnapshotStatus.USABLE,
            InspectionOutcome.HIT,
            None,
            "inspector-model",
            True,
            id="hit",
        ),
        pytest.param(
            SnapshotStatus.USABLE,
            InspectionOutcome.HIT,
            "reason on a hit",
            "inspector-model",
            False,
            id="hit-with-reason",
        ),
        pytest.param(
            SnapshotStatus.USABLE,
            InspectionOutcome.MISS,
            "nothing satisfies the brief",
            "inspector-model",
            True,
            id="miss",
        ),
        pytest.param(
            SnapshotStatus.USABLE,
            InspectionOutcome.MISS,
            None,
            "inspector-model",
            False,
            id="miss-without-reason",
        ),
        pytest.param(
            SnapshotStatus.USABLE,
            None,
            None,
            "inspector-model",
            False,
            id="usable-without-outcome",
        ),
        pytest.param(
            SnapshotStatus.USABLE,
            InspectionOutcome.HIT,
            None,
            None,
            False,
            id="usable-without-model",
        ),
        pytest.param(
            SnapshotStatus.BROKEN,
            None,
            "rejected by deterministic checks",
            None,
            True,
            id="broken-before-model",
        ),
        pytest.param(
            SnapshotStatus.BROKEN,
            None,
            "model found the snapshot unusable",
            "inspector-model",
            True,
            id="broken-by-model",
        ),
        pytest.param(
            SnapshotStatus.BROKEN,
            None,
            None,
            "inspector-model",
            False,
            id="broken-without-reason",
        ),
        pytest.param(
            SnapshotStatus.BROKEN,
            InspectionOutcome.HIT,
            None,
            "inspector-model",
            False,
            id="broken-with-outcome",
        ),
        pytest.param(
            "kaputt",
            None,
            None,
            None,
            False,
            id="status-outside-domain",
        ),
    ],
)
def test_attempt_verdict(
    funes_db: Session,
    status: str,
    outcome: str | None,
    reason: str | None,
    model: str | None,
    accepted: bool,
) -> None:
    """Only the valid verdict combinations of the pipeline store a row;
    every other combination is rejected by a check constraint."""
    candidate_id, snapshot_id = make_candidate(funes_db)
    statement = insert(attempt_table).values(
        id=uuid.uuid4(),
        candidate_id=candidate_id,
        snapshot_id=snapshot_id,
        status=status,
        outcome=outcome,
        reason=reason,
        model=model,
    )
    if accepted:
        result = funes_db.execute(statement)
        funes_db.commit()
        assert result.rowcount == 1
    else:
        with pytest.raises(IntegrityError):
            funes_db.execute(statement)


def test_attempt_snapshot_is_one_to_one(funes_db: Session) -> None:
    """A snapshot backs at most one attempt: a re-run captures its own
    snapshot instead of reusing another attempt's."""
    candidate_id, snapshot_id = make_candidate(funes_db)
    funes_db.execute(
        insert(attempt_table).values(
            id=uuid.uuid4(),
            candidate_id=candidate_id,
            snapshot_id=snapshot_id,
            status=SnapshotStatus.USABLE,
            outcome=InspectionOutcome.HIT,
            model="inspector-model",
        )
    )
    funes_db.commit()
    with pytest.raises(IntegrityError):
        funes_db.execute(
            insert(attempt_table).values(
                id=uuid.uuid4(),
                candidate_id=candidate_id,
                snapshot_id=snapshot_id,
                status=SnapshotStatus.BROKEN,
                reason="already assessed elsewhere",
            )
        )
