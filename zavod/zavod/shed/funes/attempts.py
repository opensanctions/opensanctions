"""Due-candidate selection for funes inspection runs."""

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import and_, func, or_, select

from zavod.context import Context
from zavod.shed.funes.model import (
    InspectionOutcome,
    attempt_table,
    candidate_table,
    dataset_table,
    subject_table,
)

REVISIT_INTERVAL_DAYS = 30
"""How many days a hit attempt must age before its candidate is due
for a new run. Scheduling policy, not part of the inspection brief; if
timing concerns multiply, move it to the config object."""


@dataclass(frozen=True)
class DueCandidate:
    """One candidate due for an inspection run."""

    candidate_id: uuid.UUID
    url: str
    subject_name: str


def select_due_candidates(context: Context) -> list[DueCandidate]:
    """The context dataset's candidates due for an inspection run, in
    deterministic (subject, url) order. The subject name scopes the
    inspection brief.

    A candidate is due iff it has no attempt yet, or its latest attempt
    (by ``created_at``, ties broken by the greater id) is a hit older
    than ``REVISIT_INTERVAL_DAYS``. A latest miss or broken attempt
    blocks the candidate.
    """
    ranked = select(
        attempt_table.c.candidate_id.label("candidate_id"),
        attempt_table.c.created_at.label("created_at"),
        attempt_table.c.outcome.label("outcome"),
        func.row_number()
        .over(
            partition_by=attempt_table.c.candidate_id,
            order_by=(
                attempt_table.c.created_at.desc(),
                attempt_table.c.id.desc(),
            ),
        )
        .label("rank"),
    ).subquery()
    latest = (
        select(
            ranked.c.candidate_id,
            ranked.c.created_at,
            ranked.c.outcome,
        )
        .where(ranked.c.rank == 1)
        .subquery()
    )
    cutoff = datetime.now(UTC) - timedelta(days=REVISIT_INTERVAL_DAYS)
    statement = (
        select(
            candidate_table.c.id,
            candidate_table.c.url,
            subject_table.c.name.label("subject_name"),
        )
        .join(subject_table, subject_table.c.id == candidate_table.c.subject_id)
        .join(dataset_table, dataset_table.c.id == subject_table.c.dataset_id)
        .outerjoin(latest, latest.c.candidate_id == candidate_table.c.id)
        .where(dataset_table.c.name == context.dataset.name)
        .where(
            or_(
                latest.c.candidate_id.is_(None),
                and_(
                    latest.c.created_at < cutoff,
                    latest.c.outcome == InspectionOutcome.HIT,
                ),
            )
        )
        .order_by(subject_table.c.name, candidate_table.c.url)
    )
    rows = context.db.execute(statement).all()
    return [DueCandidate(row.id, row.url, row.subject_name) for row in rows]
