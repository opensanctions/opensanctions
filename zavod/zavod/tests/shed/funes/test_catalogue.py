"""Tests for syncing funes dataset definitions into the catalogue
tables."""

from typing import Any

from sqlalchemy import func, select
from nomenklatura.db import Session

from zavod.shed.funes.catalogue import sync_catalogue
from zavod.shed.funes.definitions import DatasetDefinition
from zavod.shed.funes.model import candidate_table, dataset_table, subject_table


def definition(**overrides: Any) -> DatasetDefinition:
    data: dict[str, Any] = {
        "name": "test_dataset",
        "people_sought": "board members",
        "subject_label": "Organization",
        "subjects": [
            {
                "name": "Bank A",
                "urls": ["https://a.example/board", "https://a.example/management"],
            },
            {"name": "Bank B", "urls": ["https://b.example/board"]},
        ],
    }
    data.update(overrides)
    return DatasetDefinition.model_validate(data)


def count(session: Session, table: Any) -> int:
    return int(session.execute(select(func.count()).select_from(table)).scalar_one())


def test_sync_creates_rows(funes_db: Session) -> None:
    sync_catalogue(funes_db, definition())
    assert count(funes_db, dataset_table) == 1
    assert count(funes_db, subject_table) == 2
    assert count(funes_db, candidate_table) == 3
    people_sought = funes_db.execute(select(dataset_table.c.people_sought)).scalar_one()
    assert people_sought == "board members"


def test_resync_idempotent(funes_db: Session) -> None:
    sync_catalogue(funes_db, definition())
    sync_catalogue(funes_db, definition())
    assert count(funes_db, candidate_table) == 3


def test_brief_synced_on_change(funes_db: Session) -> None:
    sync_catalogue(funes_db, definition())
    sync_catalogue(funes_db, definition(people_sought="chief executives"))
    assert count(funes_db, dataset_table) == 1
    people_sought = funes_db.execute(select(dataset_table.c.people_sought)).scalar_one()
    assert people_sought == "chief executives"


def test_removed_subject_and_url_kept(funes_db: Session) -> None:
    sync_catalogue(funes_db, definition())
    sync_catalogue(
        funes_db,
        definition(subjects=[{"name": "Bank A", "urls": ["https://a.example/board"]}]),
    )
    # Bank B and the management URL stay in the catalogue:
    assert count(funes_db, subject_table) == 2
    assert count(funes_db, candidate_table) == 3


def test_added_subject_and_url(funes_db: Session) -> None:
    sync_catalogue(funes_db, definition())
    sync_catalogue(
        funes_db,
        definition(
            subjects=[
                {"name": "Bank A", "urls": ["https://a.example/board"]},
                {
                    "name": "Bank C",
                    "urls": ["https://c.example/board", "https://b.example/board"],
                },
            ]
        ),
    )
    assert count(funes_db, subject_table) == 3
    # Bank C contributes both its URLs, including the one Bank B already has:
    assert count(funes_db, candidate_table) == 5


def test_same_url_two_subjects(funes_db: Session) -> None:
    sync_catalogue(
        funes_db,
        definition(
            subjects=[
                {"name": "Bank A", "urls": ["https://shared.example/board"]},
                {"name": "Bank B", "urls": ["https://shared.example/board"]},
            ]
        ),
    )
    assert count(funes_db, candidate_table) == 2


def test_two_datasets_share_url(funes_db: Session) -> None:
    sync_catalogue(funes_db, definition())
    sync_catalogue(
        funes_db,
        definition(
            name="second",
            subjects=[{"name": "Bank A", "urls": ["https://a.example/board"]}],
        ),
    )
    assert count(funes_db, dataset_table) == 2
    assert count(funes_db, subject_table) == 3
    assert count(funes_db, candidate_table) == 4
