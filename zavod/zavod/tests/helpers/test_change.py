from pathlib import Path
import tempfile
from datetime import date, datetime
import pytest
import requests_mock
from lxml import html

from zavod.context import Context
from zavod.helpers.change import (
    assert_dom_hash,
    assert_file_hash,
    assert_url_hash,
    record_hash,
)


def test_assert_url_hash(vcontext: Context):
    with requests_mock.Mocker() as m:
        m.get("/banana", text="hello, world!")

        assert assert_url_hash(
            vcontext,
            "https://example.com/banana",
            "1f09d30c707d53f3d16c530dd73d70a6ce7596a9",
        )

        assert assert_url_hash(
            vcontext,
            "https://example.com/banana",
            "1f09d30c707d53f3d16c530dd73d70a6ce7596a9",
            raise_exc=True,
        )

        assert not assert_url_hash(
            vcontext,
            "https://example.com/banana",
            "banana",
        )

        with pytest.raises(AssertionError):
            assert_url_hash(
                vcontext,
                "https://example.com/banana",
                "banana",
                raise_exc=True,
            )


def test_assert_dom_hash():
    doc = html.fromstring("<html><body>hello, world!</body></html>")
    assert assert_dom_hash(doc, "4b5f0790985812c511f5d01e807c739625104288")
    assert not assert_dom_hash(doc, "1f09d30c707d53f3d16c530dd73d70a6ce7596a9")

    doc = html.fromstring("<html><body>hello,   World!</body></html>")
    assert assert_dom_hash(doc, "4b5f0790985812c511f5d01e807c739625104288")


def test_assert_file_hash():
    with tempfile.NamedTemporaryFile(delete=False) as fh:
        fh.write(b"hello, world!")
        fh.flush()
        assert assert_file_hash(
            Path(fh.name), "1f09d30c707d53f3d16c530dd73d70a6ce7596a9"
        )
        assert not assert_file_hash(Path(fh.name), "wrong hash")


def test_record_hash_structure():
    record = {"name": "Ivan", "aliases": ["Iwan", "Ivan P."], "id": 12}
    assert record_hash(record) == record_hash(
        {"id": 12, "aliases": ["Iwan", "Ivan P."], "name": "Ivan"}
    )
    # Empty values hash like missing keys or list items.
    assert record_hash(record) == record_hash(
        {**record, "notes": None, "title": " ", "tags": [], "extra": {"a": ""}}
    )
    assert record_hash(["a", None, "b"]) == record_hash(["a", "b"])
    assert record_hash(("a", "b")) == record_hash(["a", "b"])
    # List order is content.
    assert record_hash(["a", "b"]) != record_hash(["b", "a"])
    # A separator in a value doesn't collide with two values.
    assert record_hash(["a,b"]) != record_hash(["a", "b"])
    assert record_hash({"a": {"b": "c"}}) != record_hash({"a": "b"})
    assert record_hash({"a": "b\x1fc"}) == record_hash({"a": "bc"})


def test_record_hash_scalars():
    assert record_hash(" Ivan ") == record_hash("Ivan")
    assert record_hash("Ivan") != record_hash("IVAN")
    assert record_hash("Ivan  Petrov") != record_hash("Ivan Petrov")
    assert record_hash(None) == record_hash("")
    assert record_hash(1) == record_hash(1.0) == record_hash("1")
    assert record_hash(1.5) == record_hash(1.50) == record_hash("1.5")
    assert record_hash(1.256) == record_hash("1.26")
    assert record_hash(-0.001) == record_hash(0)
    assert record_hash(1.2) != record_hash(1.3)
    assert record_hash(True) == record_hash("true")
    assert record_hash(False) == record_hash("false")
    assert record_hash(datetime(2022, 3, 1)) == record_hash(date(2022, 3, 1))
    assert record_hash(date(2022, 3, 1)) == record_hash("2022-03-01")
    assert record_hash(datetime(2022, 3, 1, 12, 30)) == record_hash(
        "2022-03-01T12:30:00"
    )


def test_record_hash_rejects():
    with pytest.raises(TypeError):
        record_hash({1: "a"})
    with pytest.raises(TypeError):
        record_hash({"a": {"b"}})
    with pytest.raises(ValueError):
        record_hash(float("nan"))
