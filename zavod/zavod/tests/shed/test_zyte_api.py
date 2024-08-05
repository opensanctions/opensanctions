import pytest
import requests_mock
from base64 import b64encode

from zavod.context import Context
from zavod.meta.dataset import Dataset
from zavod.runtime.cache import get_cache, get_engine, get_metadata
from zavod.shed.zyte_api import UnblockFailedException, fetch_html, fetch_resource


def test_browser_html(testdataset1: Dataset):
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
        assert "javascript" not in request_body
        assert request_body["browserHtml"] is True, request_body
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


def test_http_response_body(testdataset1: Dataset):
    context = Context(testdataset1)

    def validator(el):
        return True

    with requests_mock.Mocker() as m:
        m.post(
            "https://api.zyte.com/v1/extract",
            json={
                "httpResponseBody": b64encode(
                    "<html><h1>Hello, World!</h1></html>".encode()
                ).decode(),
                "httpResponseHeaders": [
                    {"name": "content-type", "value": "text/html; charset=iso-8859-1"}
                ],
            },
        )
        doc = fetch_html(
            context, "https://test.com/bla", validator, html_source="httpResponseBody"
        )
        el = doc.find(".//h1")
        assert el.text == "Hello, World!"
        assert m.call_count == 1
        request = m.request_history[0]
        request_body = request.json()
        assert request_body["httpResponseBody"] is True, request_body
        assert request_body["httpResponseHeaders"] is True, request_body
        assert "javascript" not in request_body
        assert request_body["url"] == "https://test.com/bla", request_body
        assert request_body["actions"] == [], request_body

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


def test_fetch_resource(testdataset1: Dataset):
    context = Context(testdataset1)

    with requests_mock.Mocker() as m:
        m.post(
            "https://api.zyte.com/v1/extract",
            json={
                "httpResponseBody": b64encode(
                    "name,surname\nSally,Sue".encode()
                ).decode(),
                "httpResponseHeaders": [
                    {"name": "content-type", "value": "text/csv; charset=latin-1"}
                ],
            },
        )
        cached, path, media_type, charset = fetch_resource(
            context,
            "source.csv",
            "https://test.com/download.csv",
        )
        assert not cached
        assert media_type == "text/csv"
        assert charset == "latin-1"
        with open(path) as f:
            assert f.read() == "name,surname\nSally,Sue"
        assert m.call_count == 1
        request = m.request_history[0]
        request_body = request.json()
        assert request_body["httpResponseBody"] is True, request_body
        assert request_body["httpResponseHeaders"] is True, request_body
        assert request_body["url"] == "https://test.com/download.csv", request_body

        cached, path, media_type, charset = fetch_resource(
            context,
            "source.csv",
            "https://test.com/download.csv",
        )
        assert cached
        assert media_type is None
        assert charset is None
        with open(path) as f:
            assert f.read() == "name,surname\nSally,Sue"
        assert m.call_count == 1

    context.close()
