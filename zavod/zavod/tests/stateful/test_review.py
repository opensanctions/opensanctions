from pydantic import BaseModel

from zavod import settings
from zavod.context import Context
from zavod.stateful.review import get_review, request_review
from zavod.stateful.model import review_table

SOURCE_LABEL = "test"
SOURCE_URL = "http://source"
SOURCE_MIME_TYPE = "text/plain"
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
    review = request_review(
        context,
        "key1",
        SOURCE_VALUE,
        SOURCE_MIME_TYPE,
        SOURCE_LABEL,
        SOURCE_URL,
        model,
        1,
    )
    assert review.accepted is False
    row = get_row(context.conn, "key1")
    assert row is not None
    assert row["accepted"] is False


def test_current_model_version_updates_last_seen_version(testdataset1, monkeypatch):
    monkeypatch.setattr(settings, "RUN_VERSION", "20240101010101-aaa")
    context1 = Context(testdataset1)
    context1.begin(clear=True)
    context1_version = context1.version.id
    data = DummyModel(foo="bar")
    request_review(
        context1,
        "key2",
        SOURCE_VALUE,
        SOURCE_MIME_TYPE,
        SOURCE_LABEL,
        SOURCE_URL,
        data,
        1,
    )
    row = get_row(context1.conn, "key2")
    assert row and row["last_seen_version"] == context1_version
    monkeypatch.setattr(settings, "RUN_VERSION", "20240101010102-aaa")
    context2 = Context(testdataset1)
    context2.begin(clear=True)
    context2_version = context2.version.id
    review = get_review(
        context2,
        DummyModel,
        "key2",
        1,
    )
    assert review.last_seen_version == context2_version
    row = get_row(context2.conn, "key2")
    assert row["last_seen_version"] == context2_version
    assert context1_version != context2_version


def test_expired_model_version_returns_none(testdataset1, monkeypatch):
    context = Context(testdataset1)
    context.begin(clear=True)
    data = DummyModel(foo="bar")
    review = request_review(
        context,
        "key3",
        SOURCE_VALUE,
        SOURCE_MIME_TYPE,
        SOURCE_LABEL,
        SOURCE_URL,
        data,
        1,
    )
    review = get_review(
        context,
        DummyModel,
        "key3",
        1,
    )
    assert review is not None
    review = get_review(
        context,
        DummyModel,
        "key3",
        2,
    )
    assert review is None


def test_re_request_deletes_old_and_inserts_new(testdataset1, monkeypatch):
    context1 = Context(testdataset1)
    data = DummyModel(foo="bar")
    review = request_review(
        context1,
        "key3",
        SOURCE_VALUE,
        SOURCE_MIME_TYPE,
        SOURCE_LABEL,
        SOURCE_URL,
        data,
        1,
        default_accepted=True,
    )
    assert review.accepted is True
    data2 = DummyModel(foo="baz")
    context2 = Context(testdataset1)
    review = request_review(
        context2,
        "key3",
        SOURCE_VALUE,
        SOURCE_MIME_TYPE,
        SOURCE_LABEL,
        SOURCE_URL,
        data2,
        2,
        default_accepted=False,
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
    assert review.accepted is False
