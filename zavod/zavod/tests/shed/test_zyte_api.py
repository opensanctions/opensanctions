import pytest
import requests_mock

from zavod.context import Context
from zavod.meta.dataset import Dataset
from zavod.runtime.cache import get_cache, get_engine, get_metadata
from zavod.shed.zyte_api import UnblockFailedException, fetch_html


def test_fetch(testdataset1: Dataset):
    context = Context(testdataset1)

    def validator(el):
        return True

    with requests_mock.Mocker() as m:
        m.post(
            "https://api.zyte.com/v1/extract",
            json={"browserHtml": "<html><h1>Hello, World!</h1></html>"},
        )
        doc = fetch_html(context, "https://test.com/bla", validator)
        el = doc.find(".//h1")
        assert el.text == "Hello, World!"
        assert m.call_count == 1
        request = m.request_history[0]
        request_body = request.json()
        assert "javascript" not in request.json()
        assert request_body["url"] == "https://test.com/bla", request_body
        assert request_body["actions"] == [], request_body

        action = {"some": "blob"}
        doc = fetch_html(
            context,
            "https://test.com/bla",
            validator,
            actions=[action],
            javascript=True,
        )
        request2 = m.request_history[1]
        request_body2 = request2.json()
        assert request_body2["actions"] == [action], request_body2
        assert request_body2["javascript"], request_body2

    context.close()


def test_unblock_failed(testdataset1: Dataset):
    context = Context(testdataset1)

    def validator(el):
        return False

    with requests_mock.Mocker() as m:
        m.post(
            "https://api.zyte.com/v1/extract",
            json={"browserHtml": "<html><h1>Enable JS</h1></html>"},
        )
        with pytest.raises(UnblockFailedException):
            fetch_html(context, "https://test.com/bla", validator, backoff_factor=0)
        assert m.call_count == 4

    context.close()


def test_caching(testdataset1: Dataset):
    context = Context(testdataset1)

    def validator(el):
        return True

    with requests_mock.Mocker() as m:
        m.post(
            "https://api.zyte.com/v1/extract",
            json={"browserHtml": "<html><h1>Hello, World!</h1></html>"},
        )
        doc = fetch_html(context, "https://test.com/bla", validator, cache_days=14)
        el = doc.find(".//h1")
        assert el.text == "Hello, World!"
        assert m.call_count == 1

        doc = fetch_html(context, "https://test.com/bla", validator, cache_days=14)
        el = doc.find(".//h1")
        assert el.text == "Hello, World!"
        assert m.call_count == 1  # still 1 because cache hit

    context.close()
    get_cache.cache_clear()
    get_engine.cache_clear()
    get_metadata.cache_clear()
