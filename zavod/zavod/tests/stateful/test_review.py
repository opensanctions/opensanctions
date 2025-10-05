from pydantic import BaseModel

from zavod import settings
from zavod.context import Context
from zavod.stateful.review import (
    observe_source_value,
    request_review,
    review_key,
    TextSourceValue,
    LLMExtractionConfig,
)
from zavod.stateful.model import review_table


class DummyModel(BaseModel):
    foo: str


def mock_source_value(key: str) -> TextSourceValue:
    return TextSourceValue(
        key_parts=key, label="test", url="http://source", text="unstructured data"
    )


def mock_extraction_config(prompt: str) -> LLMExtractionConfig:
    return LLMExtractionConfig(
        data_model=DummyModel, llm_model="test-llm", prompt=prompt
    )


def get_row(conn, key):
    sel = review_table.select().where(
        review_table.c.key == key, review_table.c.deleted_at.is_(None)
    )
    return conn.execute(sel).mappings().first()


def test_new_key_saved_and_accepted_false(testdataset1):
    context = Context(testdataset1)
    data = DummyModel(foo="bar")
    review = request_review(
        context,
        source_value=mock_source_value("key1"),
        extraction_config=mock_extraction_config(prompt="Extract foo"),
        orig_extraction_data=data,
        crawler_version=1,
    )
    assert review.accepted is False
    row = get_row(context.conn, "key1")
    assert row is not None
    assert row["accepted"] is False


def test_current_crawler_version_updates_last_seen_version(testdataset1, monkeypatch):
    monkeypatch.setattr(settings, "RUN_VERSION", "20240101010101-aaa")
    context1 = Context(testdataset1)
    context1.begin(clear=True)
    context1_version = context1.version.id
    extraction_config = mock_extraction_config(prompt="Extract foo")
    data = DummyModel(foo="bar")
    request_review(
        context1,
        source_value=mock_source_value("key2"),
        extraction_config=extraction_config,
        orig_extraction_data=data,
        crawler_version=1,
    )
    row = get_row(context1.conn, "key2")
    assert row and row["last_seen_version"] == context1_version
    monkeypatch.setattr(settings, "RUN_VERSION", "20240101010102-aaa")
    context2 = Context(testdataset1)
    context2.begin(clear=True)
    context2_version = context2.version.id
    observation = observe_source_value(
        context2,
        source_value=mock_source_value("key2"),
        extraction_config=extraction_config,
    )
    # Updated in the db
    row = get_row(context2.conn, "key2")
    assert row["last_seen_version"] == context2_version
    assert context1_version != context2_version
    # Returned review is up to date
    assert observation.review.last_seen_version == context2_version


def test_re_request_deletes_old_and_inserts_new(testdataset1):
    """When the new data is different, the extracted_data reverts to orig_extraction_data."""
    context1 = Context(testdataset1)
    extraction_config = mock_extraction_config(prompt="Extract foo")
    data = DummyModel(foo="bar")
    review = request_review(
        context1,
        source_value=mock_source_value("key3"),
        extraction_config=extraction_config,
        orig_extraction_data=data,
        crawler_version=1,
        default_accepted=True,
    )
    assert review.accepted is True

    # Simulate a reviewer changing the extracted data
    review.extracted_data = DummyModel(foo="barrr")
    review.modified_by = "test@user.com"
    review.save(context1.conn)

    data2 = DummyModel(foo="baz")
    context2 = Context(testdataset1)
    review = request_review(
        context2,
        source_value=mock_source_value("key3"),
        extraction_config=extraction_config,
        orig_extraction_data=data2,
        crawler_version=1,
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
    assert new["extracted_data"]["foo"] == "baz"
    assert new["modified_by"] == "zavod"
    assert review.accepted is False


def test_re_request_same_keeps_accepted_and_extracted_data(testdataset1):
    """When the new data is is same as before, the extracted_data and accepted status are kept."""
    context1 = Context(testdataset1)
    extraction_config = mock_extraction_config(prompt="Extract foo")
    data = DummyModel(foo="bar")
    review = request_review(
        context1,
        source_value=mock_source_value("key4"),
        extraction_config=extraction_config,
        orig_extraction_data=data,
        crawler_version=1,
        default_accepted=False,
    )
    assert review.accepted is False

    # Simulate a reviewer changing the extracted data
    review.extracted_data = DummyModel(foo="barrr")
    review.modified_by = "test@user.com"
    review.accepted = True
    review.save(context1.conn)

    context2 = Context(testdataset1)
    review = request_review(
        context2,
        source_value=mock_source_value("key4"),
        extraction_config=extraction_config,
        orig_extraction_data=data,
        crawler_version=1,
        default_accepted=False,
    )
    old = (
        context2.conn.execute(
            review_table.select().where(
                review_table.c.key == "key4",
                review_table.c.deleted_at.is_(None),
            )
        )
        .mappings()
        .first()
    )
    assert old is not None
    new = get_row(context2.conn, "key4")
    assert new is not None
    assert new["accepted"] is True
    assert new["orig_extraction_data"]["foo"] == "bar"
    assert new["extracted_data"]["foo"] == "barrr"
    assert new["modified_by"] == "zavod"
    assert review.accepted is True


def test_review_key():
    assert review_key("key1") == "key1"
    assert review_key(["part1", "part2"]) == "part1-part2"
    assert (
        review_key("key1" * 100)
        == "key1key1key1key1key1key1key1key1key1key1key1key1key1key1key1key1key1key1key1key1-0ed37245d1"
    )
    assert (
        review_key(["key1"] * 100)
        == "key1-key1-key1-key1-key1-key1-key1-key1-key1-key1-key1-key1-key1-key1-key1-key1--5611368bed"
    )
