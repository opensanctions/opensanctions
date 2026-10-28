"""The funes catalogue: the dataset, subject and candidate rows that
make up the pipeline's units of work, and the pydantic models that
read and write them, after the pattern of ``zavod.stateful.review``:
Core-level table access through the caller's session, wrapped in model
classes with class and instance methods."""

import uuid
from datetime import UTC, datetime, timedelta

from nomenklatura.db import Session
from pydantic import BaseModel
from sqlalchemy import and_, func, or_, select

from zavod.shed.funes.definitions import DatasetDefinition
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


class Dataset(BaseModel):
    """One input dataset of the pipeline (``funes_dataset`` row)."""

    id: uuid.UUID
    name: str
    people_sought: str
    """Names the class of position holders the dataset is after."""
    subject_label: str
    """Names the subject's role in the inspection brief (e.g. Organization,
    Court, Sending country)."""
    created_at: datetime

    @classmethod
    def sync(cls, session: Session, definition: DatasetDefinition) -> "Dataset":
        """Sync one dataset definition into the catalogue tables and
        return the synced dataset.

        Called at the start of each dataset's inspection run with the
        brief validated from that dataset's metadata. The dataset brief
        columns (``people_sought``, ``subject_label``) are updated so the
        dataset metadata stays the source of truth; subjects and
        candidates are append-only and survive brief edits.

        Checkpoints the session so the catalogue rows are durable
        before the run reads its due candidates.
        """
        dataset_upsert = session.insert(dataset_table).values(
            name=definition.name,
            people_sought=definition.people_sought,
            subject_label=definition.subject_label,
        )
        session.execute(
            dataset_upsert.on_conflict_do_update(
                index_elements=["name"],
                set_={
                    "people_sought": dataset_upsert.excluded.people_sought,
                    "subject_label": dataset_upsert.excluded.subject_label,
                },
            )
        )
        row = session.execute(
            select(
                dataset_table.c.id,
                dataset_table.c.name,
                dataset_table.c.people_sought,
                dataset_table.c.subject_label,
                dataset_table.c.created_at,
            ).where(dataset_table.c.name == definition.name)
        ).one()
        dataset = cls.model_validate(dict(row._mapping))
        session.execute(
            session.insert(subject_table)
            .values(
                [
                    {"dataset_id": dataset.id, "name": subject.name}
                    for subject in definition.subjects
                ]
            )
            .on_conflict_do_nothing(index_elements=["dataset_id", "name"])
        )
        subject_id_by_name: dict[str, uuid.UUID] = dict(
            session.execute(
                select(subject_table.c.name, subject_table.c.id).where(
                    subject_table.c.dataset_id == dataset.id
                )
            ).all()
        )
        candidates = [
            {"subject_id": subject_id_by_name[subject.name], "url": url}
            for subject in definition.subjects
            for url in subject.urls
        ]
        if candidates:
            session.execute(
                session.insert(candidate_table)
                .values(candidates)
                .on_conflict_do_nothing(index_elements=["subject_id", "url"])
            )
        session.checkpoint()
        return dataset


class Candidate(BaseModel):
    """One unit of pipeline work: a URL of a subject, inspected under
    the dataset brief (``funes_candidate`` row)."""

    id: uuid.UUID
    subject_id: uuid.UUID
    url: str
    created_at: datetime
    attempt_id: uuid.UUID | None = None
    """The discovery run that selected this candidate, if any."""
    reason: str | None = None
    """The selecting model's reason, if any."""
    subject_name: str | None = None
    """Not a column of ``funes_candidate``: the subject's name, read from
    the joined subject row and populated only by ``select_due``."""

    @classmethod
    def select_due(cls, session: Session, dataset_name: str) -> list["Candidate"]:
        """The named dataset's candidates due for an inspection run, in
        deterministic (subject, url) order, with ``subject_name`` set.

        A candidate is due iff it has no attempt yet, or its latest
        attempt (by ``created_at``, ties broken by the greater id) is a
        hit older than ``REVISIT_INTERVAL_DAYS``. A latest miss or
        broken attempt blocks the candidate: repair, not selection,
        re-drives those.
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
            select(candidate_table, subject_table.c.name.label("subject_name"))
            .join(subject_table, subject_table.c.id == candidate_table.c.subject_id)
            .join(dataset_table, dataset_table.c.id == subject_table.c.dataset_id)
            .outerjoin(latest, latest.c.candidate_id == candidate_table.c.id)
            .where(dataset_table.c.name == dataset_name)
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
        rows = session.execute(statement).all()
        return [cls.model_validate(dict(row._mapping)) for row in rows]
