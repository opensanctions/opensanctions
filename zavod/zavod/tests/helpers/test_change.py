from pathlib import Path
import tempfile
import pytest
import requests_mock
from lxml import html

from zavod.context import Context
from zavod.helpers.change import assert_file_hash, assert_url_hash, assert_dom_hash


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
