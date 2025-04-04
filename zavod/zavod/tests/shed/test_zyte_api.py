import pytest
import requests_mock
from base64 import b64encode

from zavod.context import Context
from zavod.meta.dataset import Dataset
from zavod.shed.zyte_api import (
    UnblockFailedException,
    fetch_html,
    fetch_json,
    fetch_resource,
    fetch_text,
)


def test_browser_html(testdataset1: Dataset):
    context = Context(testdataset1)

    with requests_mock.Mocker() as m:
        m.post(
            "https://api.zyte.com/v1/extract",
            json={"browserHtml": "<html><h1>Hello, World!</h1></html>"},
        )
        doc = fetch_html(context, "https://test.com/bla", ".//h1")
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
            ".//h1",
            actions=[action],
            javascript=True,
        )
        request2 = m.request_history[1]
        request_body2 = request2.json()
        assert request_body2["actions"] == [action], request_body2
        assert request_body2["javascript"], request_body2

    context.close()


def test_fetch_html_http_response_body(testdataset1: Dataset):
    context = Context(testdataset1)

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
            context, "https://test.com/bla", ".//h1", html_source="httpResponseBody"
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

    with requests_mock.Mocker() as m:
        m.post(
            "https://api.zyte.com/v1/extract",
            json={"browserHtml": "<html><h1>Enable JS</h1></html>"},
        )
        with pytest.raises(UnblockFailedException):
            fetch_html(context, "https://test.com/bla", ".//div", backoff_factor=0)
        assert m.call_count == 4

    context.close()


def test_caching(testdataset1: Dataset):
    context = Context(testdataset1)

    with requests_mock.Mocker() as m:
        m.post(
            "https://api.zyte.com/v1/extract",
            json={"browserHtml": "<html><h1>Hello, World!</h1></html>"},
        )
        doc = fetch_html(context, "https://test.com/bla", ".//h1", cache_days=14)
        el = doc.find(".//h1")
        assert el.text == "Hello, World!"
        assert m.call_count == 1

        doc = fetch_html(context, "https://test.com/bla", ".//h1", cache_days=14)
        el = doc.find(".//h1")
        assert el.text == "Hello, World!"
        assert m.call_count == 1  # still 1 because cache hit

    context.close()


def test_fetch_resource(testdataset1: Dataset):
    context = Context(testdataset1)
    url = "https://test.com/download.csv"

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

        # When file is fetched, content-type header is available
        cached, media_type, charset, path = fetch_resource(context, "source.csv", url)
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

        # File already exists, not refetched.
        cached, media_type, charset, path = fetch_resource(context, "source.csv", url)
        assert cached
        assert media_type is None
        assert charset is None
        with open(path) as f:
            assert f.read() == "name,surname\nSally,Sue"
        assert m.call_count == 1

        # It can also assert content type for you
        with pytest.raises(AssertionError) as exc:
            fetch_resource(
                context, "source2.csv", url, expected_media_type="text/plain"
            )
        assert "text/csv" in str(exc.value), exc.value
        with pytest.raises(AssertionError) as exc:
            fetch_resource(context, "source3.csv", url, expected_charset="UTF-8")
        assert "latin-1" in str(exc.value), exc.value
        # Except when the file exists locally
        fetch_resource(context, "source2.csv", url, expected_media_type="text/plain")
        fetch_resource(context, "source3.csv", url, expected_charset="UTF-8")
    context.close()


def test_fetch_text(testdataset1: Dataset):
    context = Context(testdataset1)

    with requests_mock.Mocker() as m:
        m.post(
            "https://api.zyte.com/v1/extract",
            json={
                "httpResponseBody": b64encode("Hello, World!".encode()).decode(),
                "httpResponseHeaders": [
                    {"name": "content-type", "value": "text/plain; charset=utf-8"}
                ],
            },
        )
        cached, media_type, charset, text = fetch_text(
            context,
            "https://test.com/download.txt",
        )
        assert not cached
        assert media_type == "text/plain", media_type
        assert charset == "utf-8", charset
        assert text == "Hello, World!", text
        assert m.call_count == 1

    context.close()


def test_fetch_json(testdataset1: Dataset):
    context = Context(testdataset1)

    with requests_mock.Mocker() as m:
        m.post(
            "https://api.zyte.com/v1/extract",
            json={
                "httpResponseBody": b64encode('{"name": "Sally"}'.encode()).decode(),
                "httpResponseHeaders": [
                    {"name": "content-type", "value": "application/json; charset=utf-8"}
                ],
            },
        )
        data = fetch_json(
            context,
            "https://test.com/data.json",
        )
        assert data == {"name": "Sally"}
        assert m.call_count == 1

        request = m.request_history[0]
        request_body = request.json()
        req_heads = request_body["customHttpRequestHeaders"]
        expected_req_heads = [{"name": "Accept", "value": "application/json"}]
        assert req_heads == expected_req_heads, request_body

    context.close()


def test_fetch_json_expect_json(testdataset1: Dataset):
    context = Context(testdataset1)

    with requests_mock.Mocker() as m:
        m.post(
            "https://api.zyte.com/v1/extract",
            json={
                "httpResponseBody": b64encode("Go away".encode()).decode(),
                "httpResponseHeaders": [
                    {"name": "content-type", "value": "text/html; charset=utf-8"}
                ],
            },
        )
        with pytest.raises(AssertionError) as exc:
            fetch_json(
                context,
                "https://test.com/data.json",
            )
        assert "text/html" in str(exc.value), exc.value
