from lxml import html
from rigour.time import utc_now
from pydantic import BaseModel
from followthemoney.dataset import Version

from zavod import settings
from zavod.context import Context
from zavod.meta import Dataset
from zavod.stateful.model import review_table
from zavod.stateful.review import (
    HtmlSourceValue,
    Review,
    TextSourceValue,
    review_extraction,
    review_key,
)


class DummyModel(BaseModel):
    foo: str


def mock_source_value(key: str) -> TextSourceValue:
    return TextSourceValue(
        key_parts=key, label="test", url="http://source", text="unstructured data"
    )


def get_row(conn, key):
    sel = review_table.select().where(
        review_table.c.key == key, review_table.c.deleted_at.is_(None)
    )
    return conn.execute(sel).mappings().first()


def get_all_rows(conn, key):
    sel = review_table.select().where(review_table.c.key == key)
    return list(conn.execute(sel).mappings().all())


def test_new_key_saved_and_accepted_false(testdataset1: Dataset):
    context = Context(testdataset1, settings.RUN_VERSION)
    data = DummyModel(foo="bar")
    review = review_extraction(
        context,
        crawler_version=1,
        source_value=mock_source_value("key1"),
        original_extraction=data,
        origin="test_data",
    )

    assert review.accepted is False
    context.flush()
    row = get_row(context.db, review_key("key1"))
    assert row is not None
    assert row["accepted"] is False
    context.close()


def test_no_change_updates_last_seen_version(testdataset1, monkeypatch):
    #   preconditions:
    #     - same crawler version
    #     - source hasn't changed
    #   postconditions:
    #     - no new row
    #     - last_seen_version updated
    monkeypatch.setattr(
        settings, "RUN_VERSION", Version.from_string("20240101010101-aaa")
    )
    context1 = Context(testdataset1, settings.RUN_VERSION)
    context1.begin(clear=True)
    context1_version = context1.version.id
    source_value = mock_source_value("key2")
    extracted_data = DummyModel(foo="bar")
    review_extraction(
        context1,
        crawler_version=1,
        source_value=source_value,
        original_extraction=extracted_data,
        origin="test data",
    )
    key2 = review_key("key2")
    row = get_row(context1.db, key2)
    assert row and row["last_seen_version"] == context1_version
    monkeypatch.setattr(
        settings, "RUN_VERSION", Version.from_string("20240101010102-aaa")
    )
    context2 = Context(testdataset1, settings.RUN_VERSION)
    context2.begin(clear=True)
    context2_version = context2.version.id
    review2 = review_extraction(
        context2,
        crawler_version=1,
        source_value=source_value,
        original_extraction=extracted_data,
        origin="test data",
    )
    # Updated in the db
    row = get_row(context2.db, key2)
    assert row["last_seen_version"] == context2_version
    assert context1_version != context2_version
    assert len(get_all_rows(context2.db, key2)) == 1
    # Returned review is up to date
    assert review2.last_seen_version == context2_version
    context1.close()
    context2.close()


def test_source_changed_resets_review(testdataset1: Dataset):
    #   preconditions:
    #     - there is an existing accepted review
    #     - the source AND extraction have changed
    #     - default_accepted is False
    #   postconditions:
    #     - a new version of the review is created
    #     - accepted is False
    #     - original_extraction is the new data
    #     - extracted_data is still the previous extracted_data
    #
    #   Also testing that original default_accepted=True saves,
    #   but the resetting crawl can set it to False.

    context1 = Context(testdataset1, settings.RUN_VERSION)
    source_value1 = TextSourceValue(
        key_parts="key3", label="test", url="http://s", text="bar"
    )
    data1 = DummyModel(foo="bar")
    review = review_extraction(
        context1,
        crawler_version=1,
        source_value=source_value1,
        original_extraction=data1,
        origin="test data",
        default_accepted=True,  # e.g. backfilled from a lookup or regex
    )
    assert review.accepted is True

    # Simulate a reviewer changing the extracted data
    review.extracted_data = DummyModel(foo="barrr")
    review.modified_by = "test@user.com"
    review.save(context1.db, new_revision=True)
    key3 = review_key("key3")
    row = get_row(context1.db, key3)
    assert row["modified_by"] == "test@user.com"

    source_value2 = TextSourceValue(
        key_parts="key3", label="test", url="http://s", text="baz"
    )
    data2 = DummyModel(foo="baz")
    context2 = Context(testdataset1, settings.RUN_VERSION)
    review = review_extraction(
        context2,
        crawler_version=1,
        source_value=source_value2,
        original_extraction=data2,
        origin="test data",
        default_accepted=False,
    )
    new = get_row(context2.db, key3)
    assert new["accepted"] is False
    assert new["original_extraction"]["foo"] == "baz"
    assert new["extracted_data"]["foo"] == "baz"
    assert new["modified_by"] == "zavod"
    assert review.accepted is False
    assert len(get_all_rows(context2.db, key3)) == 3  # original, edit, reset
    context1.close()
    context2.close()


def test_unaccepted_updates_original_extraction(testdataset1: Dataset):
    #   preconditions:
    #     - there is an existing unaccepted review
    #     - the source hasn't changed but extraction has
    #   postconditions:
    #     - a new version of the review is created
    #     - original_extraction is the new data
    #     - extracted_data is is the new data
    context1 = Context(testdataset1, settings.RUN_VERSION)
    data1 = DummyModel(foo="foo")
    review1 = review_extraction(
        context1,
        crawler_version=1,
        source_value=mock_source_value("key4"),
        original_extraction=data1,
        origin="test data",
    )
    key4 = review_key("key4")
    assert review1.accepted is False
    row1 = get_row(context1.db, key4)
    assert row1 is not None
    assert row1["accepted"] is False
    assert row1["original_extraction"]["foo"] == "foo"
    assert row1["extracted_data"]["foo"] == "foo"

    context2 = Context(testdataset1, settings.RUN_VERSION)
    data2 = DummyModel(foo="bar")
    review_extraction(
        context2,
        crawler_version=1,
        source_value=mock_source_value("key4"),
        original_extraction=data2,
        origin="test data",
    )
    row2 = get_row(context2.db, key4)
    assert row2 is not None
    assert row2["accepted"] is False
    assert row2["original_extraction"]["foo"] == "bar"
    assert row2["extracted_data"]["foo"] == "bar"
    assert row2["modified_by"] == "zavod"
    # Expect this to be updated in-place because it's unaccepted.
    assert len(get_all_rows(context2.db, key4)) == 1
    context1.close()
    context2.close()


def test_crawler_version_bump_resets_review(testdataset1: Dataset):
    context1 = Context(testdataset1, settings.RUN_VERSION)
    review = review_extraction(
        context1,
        crawler_version=1,
        source_value=mock_source_value("key5"),
        original_extraction=DummyModel(foo="bar"),
        origin="test data",
        default_accepted=True,  # e.g. backfilled from a lookup or regex
    )
    assert review.accepted is True

    # Simulate a reviewer changing the extracted data
    review.extracted_data = DummyModel(foo="barrr")
    review.modified_by = "test@user.com"
    review.save(context1.db, new_revision=True)
    key5 = review_key("key5")
    row = get_row(context1.db, key5)
    assert row["modified_by"] == "test@user.com"

    class ClevererModel(BaseModel):
        foo: str
        baz: str  # baz is required in the new model -> backward incompatible

    context2 = Context(testdataset1, settings.RUN_VERSION)
    review = review_extraction(
        context2,
        crawler_version=2,
        source_value=mock_source_value("key5"),
        original_extraction=ClevererModel(foo="bar", baz="zab"),
        origin="test data",
        default_accepted=False,
    )
    new = get_row(context2.db, key5)
    assert new["accepted"] is False
    assert new["original_extraction"]["foo"] == "bar"
    assert new["original_extraction"]["baz"] == "zab"
    assert new["extracted_data"]["foo"] == "bar"
    assert new["extracted_data"]["baz"] == "zab"
    assert new["modified_by"] == "zavod"
    assert review.accepted is False
    assert len(get_all_rows(context2.db, key5)) == 3  # original, edit, reset
    context1.close()
    context2.close()


def test_html_source_comparison(testdataset1: Dataset):
    source_value1 = HtmlSourceValue(
        key_parts="key6",
        label="test",
        url="http://s",
        element=html.fromstring("<html><body>foo<span>bar</span></body></html>"),
    )
    source_value2 = HtmlSourceValue(
        key_parts="key6",
        label="test",
        url="http://s",
        element=html.fromstring("<html><body>foo<span>baz</span></body></html>"),
    )
    review = Review(
        key="key6",
        dataset=testdataset1.name,
        crawler_version=1,
        source_mime_type=source_value1.mime_type,
        source_label=source_value1.label,
        source_url=source_value1.url,
        source_value=source_value1.value_string,
        data_model=DummyModel,
        extraction_schema=DummyModel.model_json_schema(),
        original_extraction=DummyModel(foo="bar"),
        extracted_data=DummyModel(foo="bar"),
        accepted=False,
        last_seen_version="123",
        modified_by="test@user.com",
        modified_at=utc_now(),
    )
    assert source_value1.matches(review)
    assert not source_value2.matches(review)


def test_text_source_comparison(testdataset1: Dataset):
    source_value1 = TextSourceValue(
        key_parts="key7", label="test", url="http://s", text="foo bar"
    )
    source_value1dot = TextSourceValue(
        key_parts="key7", label="test", url="http://s", text="foo. bar."
    )
    source_value2 = TextSourceValue(
        key_parts="key7", label="test", url="http://s", text="foo baz"
    )
    review = Review(
        key="key7",
        dataset=testdataset1.name,
        crawler_version=1,
        source_mime_type=source_value1.mime_type,
        source_label=source_value1.label,
        source_url=source_value1.url,
        source_value=source_value1.value_string,
        data_model=DummyModel,
        extraction_schema=DummyModel.model_json_schema(),
        original_extraction=DummyModel(foo="bar"),
        extracted_data=DummyModel(foo="bar"),
        accepted=False,
        last_seen_version="123",
        modified_by="test@user.com",
        modified_at=utc_now(),
    )
    assert source_value1.matches(review)
    assert source_value1dot.matches(review)
    assert not source_value2.matches(review)


def test_source_changed_updates_source_fields(testdataset1: Dataset):
    #   preconditions:
    #     - there is an existing review
    #     - the source value has changed
    #   postconditions:
    #     - source_value, source_mime_type, source_label, source_url are all
    #       updated to reflect the new source
    context1 = Context(testdataset1, settings.RUN_VERSION)
    source_value1 = TextSourceValue(
        key_parts="key8", label="label-old", url="http://s/old", text="old text"
    )
    review_extraction(
        context1,
        crawler_version=1,
        source_value=source_value1,
        original_extraction=DummyModel(foo="old"),
        origin="test data",
        default_accepted=True,
    )

    context2 = Context(testdataset1, settings.RUN_VERSION)
    source_value2 = TextSourceValue(
        key_parts="key8", label="label-new", url="http://s/new", text="new text"
    )
    review_extraction(
        context2,
        crawler_version=1,
        source_value=source_value2,
        original_extraction=DummyModel(foo="new"),
        origin="test data",
    )

    key8 = review_key("key8")
    row = get_row(context2.db, key8)
    assert row is not None
    assert row["source_value"] == source_value2.value_string
    assert row["source_mime_type"] == source_value2.mime_type
    assert row["source_label"] == source_value2.label
    assert row["source_url"] == source_value2.url
    context1.close()
    context2.close()


def test_review_key():
    # Returns a 40-char hex SHA1
    assert len(review_key("key1")) == 40
    assert all(c in "0123456789abcdef" for c in review_key("key1"))
    # Deterministic
    assert review_key("key1") == review_key("key1")
    assert review_key(["part1", "part2"]) == review_key(["part1", "part2"])
    # Parts hashed separately — order matters at this level
    assert review_key(["part1", "part2"]) != review_key(["part2", "part1"])
    # Whitespace stripped from each part
    assert review_key([" part1 ", "part2"]) == review_key(["part1", "part2"])
