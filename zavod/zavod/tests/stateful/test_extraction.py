import pytest
from pydantic import BaseModel
from zavod import settings
from zavod.stateful.model import extraction_table
from zavod.stateful.extraction import extract_items
from zavod.context import Context
from nomenklatura.versions import Version
from datetime import datetime
import os

class DummyModel(BaseModel):
    foo: str

def get_row(conn, key):
    sel = extraction_table.select().where(extraction_table.c.key == key, extraction_table.c.deleted_at == None)
    return conn.execute(sel).mappings().first()

def test_new_key_saved_and_accepted_false(testdataset1):
    context = Context(testdataset1)
    model = DummyModel(foo="bar")
    result = extract_items(context, "key1", model, "http://source")
    assert result is None
    row = get_row(context.conn, "key1")
    assert row is not None
    assert row["accepted"] is False

def test_same_hash_updates_last_seen_version(testdataset1, monkeypatch):
    monkeypatch.setattr(settings, "RUN_VERSION", "20240101010101-aaa")
    context1 = Context(testdataset1)
    context1.begin(clear=True)
    context1_version = context1.version.id
    model = DummyModel(foo="bar")
    extract_items(context1, "key2", model, "http://source")
    context1.conn.execute(
        extraction_table.update()
        .where(extraction_table.c.key == "key2")
        .values(accepted=True, extracted_data={"foo": "baz"})
    )
    row = get_row(context1.conn, "key2")
    assert row["last_seen_version"] == context1_version
    monkeypatch.setattr(settings, "RUN_VERSION", "20240101010102-aaa")
    context2 = Context(testdataset1)
    context2.begin(clear=True)
    context2_version = context2.version.id
    result = extract_items(context2, "key2", model, "http://source")
    row = get_row(context2.conn, "key2")
    assert row is not None
    assert row["accepted"] is True
    assert row["extracted_data"] == {"foo": "baz"}
    assert result == DummyModel(foo="baz")
    assert row["last_seen_version"] == context2_version
    assert context1_version != context2_version

def test_different_hash_marks_old_deleted_and_inserts_new(testdataset1, monkeypatch):
    context1 = Context(testdataset1)
    model = DummyModel(foo="bar")
    extract_items(context1, "key3", model, "http://source")
    context1.conn.execute(
        extraction_table.update()
        .where(extraction_table.c.key == "key3")
        .values(accepted=True)
    )
    model2 = DummyModel(foo="baz")
    context2 = Context(testdataset1)
    result = extract_items(context2, "key3", model2, "http://source")
    old = context2.conn.execute(
        extraction_table.select().where(extraction_table.c.key == "key3", extraction_table.c.deleted_at != None)
    ).mappings().first()
    assert old is not None
    new = get_row(context2.conn, "key3")
    assert new is not None
    assert new["accepted"] is False
    assert new["orig_extraction_data"]["foo"] == "baz"
    assert result is None
