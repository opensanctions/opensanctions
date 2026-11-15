from datetime import datetime

import pytest
from sqlalchemy import select

from zavod import helpers as h
from zavod import settings
from zavod.meta import Dataset
from zavod.stateful.model import observation_table
from zavod.stateful.observations import observe
from zavod.tests.util import make_context

T1 = datetime(2024, 1, 1)
T2 = datetime(2024, 2, 1)
T3 = datetime(2024, 3, 1)
T4 = datetime(2024, 4, 1)


def test_observe_first_seen(testdataset1: Dataset, zavod_db):
    db = make_context(testdataset1).db
    assert observe(db, "ds", "K1", "", T1) == T1
    assert observe(db, "ds", "K1", "", T2) == T1
    rows = db.execute(select(observation_table)).fetchall()
    assert len(rows) == 1
    assert rows[0].first_seen == T1
    assert rows[0].last_seen == T2

    # An out-of-order sighting doesn't move last_seen backwards.
    assert observe(db, "ds", "K1", "", T1) == T1
    row = db.execute(select(observation_table)).one()
    assert row.last_seen == T2


def test_observe_checksum_change(testdataset1: Dataset, zavod_db):
    db = make_context(testdataset1).db
    assert observe(db, "ds", "K1", "a", T1) == T1
    assert observe(db, "ds", "K1", "a", T2) == T1
    assert observe(db, "ds", "K1", "b", T3) == T3
    assert observe(db, "ds", "K1", "b", T4) == T3


def test_observe_flip_back(testdataset1: Dataset, zavod_db):
    db = make_context(testdataset1).db
    assert observe(db, "ds", "K1", "a", T1) == T1
    assert observe(db, "ds", "K1", "b", T2) == T2
    assert observe(db, "ds", "K1", "a", T3) == T3
    assert observe(db, "ds", "K1", "a", T4) == T3


def test_observe_isolation(testdataset1: Dataset, zavod_db):
    db = make_context(testdataset1).db
    assert observe(db, "ds1", "K1", "a", T1) == T1
    assert observe(db, "ds2", "K1", "a", T2) == T2
    assert observe(db, "ds1", "K2", "a", T3) == T3
    # The key-only row doesn't count as a checksum for the flip-back rule.
    assert observe(db, "ds1", "K1", "", T3) == T3
    assert observe(db, "ds1", "K1", "a", T4) == T1


def test_observe_helpers(testdataset1: Dataset, zavod_db):
    context = make_context(testdataset1)
    assert h.observe_item(context, "K1") == settings.RUN_TIME
    assert h.observe_change(context, "K1", "a") == settings.RUN_TIME
    rows = context.db.execute(select(observation_table)).fetchall()
    assert len(rows) == 2
    assert {r.dataset for r in rows} == {testdataset1.name}

    with pytest.raises(AssertionError):
        h.observe_item(context, "")
    with pytest.raises(AssertionError):
        h.observe_change(context, "K1", "")
