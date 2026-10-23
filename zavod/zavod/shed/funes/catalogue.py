"""Sync funes dataset definitions into the catalogue tables."""

import uuid

from sqlalchemy import select

from zavod.context import Context
from zavod.shed.funes.definitions import DatasetDefinition
from zavod.shed.funes.model import candidate_table, dataset_table, subject_table


def sync_catalogue(context: Context, definition: DatasetDefinition) -> None:
    """Sync one dataset definition into the catalogue tables.

    Called at the start of each dataset's inspection run with the brief
    validated from that dataset's metadata. The dataset brief columns
    (``people_sought``, ``subject_label``) are updated so the dataset
    metadata stays the source of truth; subjects and candidates are
    append-only and survive brief edits.
    """
    session = context.db
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
    dataset_id: uuid.UUID = session.execute(
        select(dataset_table.c.id).where(dataset_table.c.name == definition.name)
    ).scalar_one()
    session.execute(
        session.insert(subject_table)
        .values(
            [
                {"dataset_id": dataset_id, "name": subject.name}
                for subject in definition.subjects
            ]
        )
        .on_conflict_do_nothing(index_elements=["dataset_id", "name"])
    )
    subject_id_by_name: dict[str, uuid.UUID] = dict(
        session.execute(
            select(subject_table.c.name, subject_table.c.id).where(
                subject_table.c.dataset_id == dataset_id
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
    context.flush()
