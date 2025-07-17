from pydantic import BaseModel

from zavod import settings
from zavod.context import Context
from zavod.stateful.review import get_accepted_data
from zavod.stateful.model import review_table

SOURCE_LABEL = "test"
SOURCE_URL = "http://source"
SOURCE_CONTENT_TYPE = "text/plain"
SOURCE_VALUE = "unstructured data"


class DummyModel(BaseModel):
    foo: str


def get_row(conn, key):
    sel = review_table.select().where(
        review_table.c.key == key, review_table.c.deleted_at.is_(None)
    )
    return conn.execute(sel).mappings().first()


def test_new_key_saved_and_accepted_false(testdataset1):
    context = Context(testdataset1)
    model = DummyModel(foo="bar")
    result = get_accepted_data(
        context,
        "key1",
        SOURCE_VALUE,
        SOURCE_CONTENT_TYPE,
        SOURCE_LABEL,
        SOURCE_URL,
        model,
    )
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
    get_accepted_data(
        context1,
        "key2",
        SOURCE_VALUE,
        SOURCE_CONTENT_TYPE,
        SOURCE_LABEL,
        SOURCE_URL,
        model,
    )
    context1.conn.execute(
        review_table.update()
        .where(review_table.c.key == "key2")
        .values(accepted=True, extracted_data={"foo": "baz"})
    )
    row = get_row(context1.conn, "key2")
    assert row and row["last_seen_version"] == context1_version
    monkeypatch.setattr(settings, "RUN_VERSION", "20240101010102-aaa")
    context2 = Context(testdataset1)
    context2.begin(clear=True)
    context2_version = context2.version.id
    result = get_accepted_data(
        context2,
        "key2",
        SOURCE_VALUE,
        SOURCE_CONTENT_TYPE,
        SOURCE_LABEL,
        SOURCE_URL,
        model,
    )
    row = get_row(context2.conn, "key2")
    assert row is not None
    assert row["accepted"] is True
    assert row["extracted_data"] == {"foo": "baz"}
    print(type(result))
    assert result == DummyModel(foo="baz")
    assert row["last_seen_version"] == context2_version
    assert context1_version != context2_version


def test_different_hash_marks_old_deleted_and_inserts_new(testdataset1, monkeypatch):
    context1 = Context(testdataset1)
    model = DummyModel(foo="bar")
    get_accepted_data(
        context1,
        "key3",
        SOURCE_VALUE,
        SOURCE_CONTENT_TYPE,
        SOURCE_LABEL,
        SOURCE_URL,
        model,
    )
    context1.conn.execute(
        review_table.update().where(review_table.c.key == "key3").values(accepted=True)
    )
    model2 = DummyModel(foo="baz")
    context2 = Context(testdataset1)
    result = get_accepted_data(
        context2,
        "key3",
        SOURCE_VALUE,
        SOURCE_CONTENT_TYPE,
        SOURCE_LABEL,
        SOURCE_URL,
        model2,
    )
    old = (
        context2.conn.execute(
            review_table.select().where(
                review_table.c.key == "key3",
                review_table.c.deleted_at.is_(None),
            )
        )
        .mappings()
        .first()
    )
    assert old is not None
    new = get_row(context2.conn, "key3")
    assert new is not None
    assert new["accepted"] is False
    assert new["orig_extraction_data"]["foo"] == "baz"
    assert result is None
