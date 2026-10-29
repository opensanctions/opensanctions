"""Selection of funes candidates due for an inspection run, from the
attempt verdict history."""

import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import Row, and_, or_, select

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


def select_due_candidates(context: Context) -> list[Row[tuple[uuid.UUID, str]]]:
    """The context dataset's candidates due for an inspection run, as
    ``(candidate_id, url)`` rows in deterministic (subject, url) order.

    A candidate is due iff it has no attempt yet, or its latest attempt
    (by ``created_at``, ties broken by the greater id) is a hit older
    than ``REVISIT_INTERVAL_DAYS``. A latest miss or broken attempt
    blocks the candidate.
    """
    latest_id = (
        select(attempt_table.c.id)
        .where(attempt_table.c.candidate_id == candidate_table.c.id)
        .order_by(attempt_table.c.created_at.desc(), attempt_table.c.id.desc())
        .limit(1)
        .correlate(candidate_table)
        .scalar_subquery()
    )
    cutoff = datetime.now(UTC) - timedelta(days=REVISIT_INTERVAL_DAYS)
    statement = (
        select(candidate_table.c.id, candidate_table.c.url)
        .join(subject_table, subject_table.c.id == candidate_table.c.subject_id)
        .join(dataset_table, dataset_table.c.id == subject_table.c.dataset_id)
        .outerjoin(attempt_table, attempt_table.c.id == latest_id)
        .where(dataset_table.c.name == context.dataset.name)
        .where(
            or_(
                attempt_table.c.id.is_(None),
                and_(
                    attempt_table.c.created_at < cutoff,
                    attempt_table.c.outcome == InspectionOutcome.HIT,
                ),
            )
        )
        .order_by(subject_table.c.name, candidate_table.c.url)
    )
    return list(context.db.execute(statement).all())
