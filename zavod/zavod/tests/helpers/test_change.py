import pytest
import requests_mock
from lxml import html

from zavod.context import Context
from zavod.helpers.change import assert_url_hash, assert_dom_hash


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
    assert assert_dom_hash(doc, "da39a3ee5e6b4b0d3255bfef95601890afd80709")
    assert not assert_dom_hash(doc, "1f09d30c707d53f3d16c530dd73d70a6ce7596a9")

    doc = html.fromstring("<html><body>hello,   World!</body></html>")
    assert assert_dom_hash(doc, "da39a3ee5e6b4b0d3255bfef95601890afd80709")
