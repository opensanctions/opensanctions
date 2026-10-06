"""Tests for syncing funes dataset definitions into the catalogue
tables."""

from typing import Any

from nomenklatura.db import Session
from sqlalchemy import select

from zavod.meta import Dataset
from zavod.shed.funes.catalogue import sync_catalogue
from zavod.shed.funes.definitions import DatasetDefinition
from zavod.shed.funes.model import candidate_table, dataset_table, subject_table
from zavod.tests.util import make_context


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


def test_sync_creates_rows(testdataset1: Dataset, funes_db: Session) -> None:
    context = make_context(testdataset1)
    sync_catalogue(context, definition())
    assert len(context.db.execute(dataset_table.select()).fetchall()) == 1
    assert len(context.db.execute(subject_table.select()).fetchall()) == 2
    assert len(context.db.execute(candidate_table.select()).fetchall()) == 3
    people_sought = context.db.execute(
        select(dataset_table.c.people_sought)
    ).scalar_one()
    assert people_sought == "board members"


def test_resync_idempotent(testdataset1: Dataset, funes_db: Session) -> None:
    context = make_context(testdataset1)
    sync_catalogue(context, definition())
    sync_catalogue(context, definition())
    assert len(context.db.execute(candidate_table.select()).fetchall()) == 3


def test_brief_synced_on_change(testdataset1: Dataset, funes_db: Session) -> None:
    context = make_context(testdataset1)
    sync_catalogue(context, definition())
    sync_catalogue(context, definition(people_sought="chief executives"))
    assert len(context.db.execute(dataset_table.select()).fetchall()) == 1
    people_sought = context.db.execute(
        select(dataset_table.c.people_sought)
    ).scalar_one()
    assert people_sought == "chief executives"


def test_removed_subject_and_url_kept(testdataset1: Dataset, funes_db: Session) -> None:
    context = make_context(testdataset1)
    sync_catalogue(context, definition())
    sync_catalogue(
        context,
        definition(subjects=[{"name": "Bank A", "urls": ["https://a.example/board"]}]),
    )
    # Bank B and the management URL stay in the catalogue:
    assert len(context.db.execute(subject_table.select()).fetchall()) == 2
    assert len(context.db.execute(candidate_table.select()).fetchall()) == 3


def test_added_subject_and_url(testdataset1: Dataset, funes_db: Session) -> None:
    context = make_context(testdataset1)
    sync_catalogue(context, definition())
    sync_catalogue(
        context,
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
    assert len(context.db.execute(subject_table.select()).fetchall()) == 3
    # Bank C contributes both its URLs, including the one Bank B already has:
    assert len(context.db.execute(candidate_table.select()).fetchall()) == 5


def test_same_url_two_subjects(testdataset1: Dataset, funes_db: Session) -> None:
    context = make_context(testdataset1)
    sync_catalogue(
        context,
        definition(
            subjects=[
                {"name": "Bank A", "urls": ["https://shared.example/board"]},
                {"name": "Bank B", "urls": ["https://shared.example/board"]},
            ]
        ),
    )
    assert len(context.db.execute(candidate_table.select()).fetchall()) == 2


def test_two_datasets_share_url(testdataset1: Dataset, funes_db: Session) -> None:
    context = make_context(testdataset1)
    sync_catalogue(context, definition())
    sync_catalogue(
        context,
        definition(
            name="second",
            subjects=[{"name": "Bank A", "urls": ["https://a.example/board"]}],
        ),
    )
    assert len(context.db.execute(dataset_table.select()).fetchall()) == 2
    assert len(context.db.execute(subject_table.select()).fetchall()) == 3
    assert len(context.db.execute(candidate_table.select()).fetchall()) == 4
