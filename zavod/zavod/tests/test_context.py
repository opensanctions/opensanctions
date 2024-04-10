from typing import cast
from datetime import datetime

import pytest
import requests_mock
from requests.adapters import HTTPAdapter
import orjson

from zavod import settings
from zavod.context import Context
from zavod.meta import Dataset
from zavod.entity import Entity
from zavod.http import request_hash
from zavod.crawl import crawl_dataset
from zavod.archive import iter_dataset_statements
from zavod.runtime.sink import DatasetSink
from zavod.exc import RunFailedException
from zavod.runtime.loader import load_entry_point
from zavod.tests.conftest import XML_DOC


def test_context_helpers(testdataset1: Dataset):
    context = Context(testdataset1)
    assert context.dataset == testdataset1
    assert "docs.google.com" in context.data_url
    assert testdataset1.name in repr(context)
    gen_id = "osv-d5fdc7f711d0d9fd15421102d272e475a236005c"
    assert context.make_id("john", "doe") == gen_id
    other_prefix_id = "other-d5fdc7f711d0d9fd15421102d272e475a236005c"
    assert context.make_id("john", "doe", prefix="other") == other_prefix_id
    other_hash_prefix_id = "osv-b47c69d4529998a124703956a9a9d8ae85f4860c"
    assert context.make_id("john", "doe", hash_prefix="other") == other_hash_prefix_id
    assert context.make_id("") is None
    assert context.make_slug("john", "doe") == "osv-john-doe"
    assert context.make_slug(None) is None

    entity = context.make("Person")
    assert isinstance(entity, Entity)
    assert entity.schema.name == "Person"
    assert entity.dataset == testdataset1

    with pytest.raises(ValueError, match="Entity has no ID.+"):
        context.emit(entity)

    # no properties:
    entity.id = "test-id"
    before = context.stats.entities
    context.emit(entity)
    assert context.stats.entities == before

    result = context.lookup("plants", "banana")
    assert result is not None
    assert result.value == "Fruit"
    assert context.lookup_value("plants", "potato") == "Vegetable"
    assert context.lookup_value("plants", "stone") is None

    context.inspect(None)
    context.inspect("foo")

    # don't know how to assert anything here
    context.audit_data({"test": "bla", "foo": 3}, ignore=["foo"])

    assert context.data_time == settings.RUN_TIME
    assert context.data_time_iso == settings.RUN_TIME.isoformat(
        sep="T", timespec="seconds"
    )
    other = datetime(2020, 1, 1)
    context.data_time = other
    assert context.data_time_iso == other.isoformat(sep="T", timespec="seconds")


def test_context_dry_run(testdataset1: Dataset):
    context = Context(testdataset1, dry_run=True)
    assert context.dataset == testdataset1
    context.begin(clear=True)
    assert context.dry_run
    context.log.error("Test error")
    context.close()
    assert list(context.issues.all()) == []


def test_context_get_fetchers(testdataset1: Dataset):
    context = Context(testdataset1)

    with requests_mock.Mocker() as m:
        m.get("/bla", text="Hello, World!")
        text = context.fetch_text("https://test.com/bla", cache_days=14)
        assert text == "Hello, World!"

    text = context.fetch_text("https://test.com/bla", cache_days=14)
    assert text == "Hello, World!"

    # Extra check that cache is there
    fingerprint = request_hash("https://test.com/bla", method="GET")
    assert context.cache.get(fingerprint, max_age=14) is not None

    with requests_mock.Mocker() as m:
        m.get("/bla", json={"msg": "Hello, World!"})
        data = context.fetch_json("https://test.com/bla")
        assert data["msg"] == "Hello, World!"

    with requests_mock.Mocker() as m:
        html = "<html><h1>Hello, World!</h1></html>"
        m.get("/bla", text=html)
        doc = context.fetch_html("https://test.com/bla")
        assert doc.findtext(".//h1") == "Hello, World!"

    long = "Hello, World!\n" * 1000
    with requests_mock.Mocker() as m:
        m.get("/bla", text=long)
        path = context.fetch_resource("world.txt", "https://test.com/bla")
    assert path.stem == "world"
    with open(path, "r") as fh:
        assert fh.read() == long

    path = context.get_resource_path("doc.xml")
    with open(path, "w") as fh:
        with open(XML_DOC, "r") as src:
            fh.write(src.read())
    doc = context.parse_resource_xml("doc.xml")
    assert "MyAddress" in doc.getroot().tag

    adapter = cast(HTTPAdapter, context.http.get_adapter("https://test.com"))
    assert adapter.max_retries.total == 1
    assert adapter.max_retries.backoff_factor == 0.5
    assert adapter.max_retries.status_forcelist == [418]
    assert adapter.max_retries.allowed_methods == ["POST"]

    context.close()


def test_context_post_fetchers(testdataset1: Dataset):
    context = Context(testdataset1)

    with requests_mock.Mocker() as m:
        m.post("/bla", text="Hello, World!")
        text = context.fetch_text(
            "https://test.com/bla", cache_days=14, method="POST", data={"foo": "bar"}
        )
        assert text == "Hello, World!"

    # Testing caching
    text = context.fetch_text(
        "https://test.com/bla", cache_days=14, method="POST", data={"foo": "bar"}
    )
    assert text == "Hello, World!"

    # Testing cache miss
    with requests_mock.Mocker() as m:
        m.post("/bla", text="Not Hello, not World!")

        # That one comes from cache:
        text = context.fetch_text(
            "https://test.com/bla", cache_days=14, method="POST", data={"foo": "bar"}
        )
        assert text == "Hello, World!"

        # That one misses the cache because of different data:
        text = context.fetch_text(
            "https://test.com/bla", cache_days=14, method="POST", data={"fooz": "barz"}
        )
        assert text == "Not Hello, not World!"

    with requests_mock.Mocker() as m:
        m.post("/bla", json={"msg": "Hello, World!"})
        data = context.fetch_json("https://test.com/bla", method="POST")
        assert data["msg"] == "Hello, World!"

    with requests_mock.Mocker() as m:
        html = "<html><h1>Hello, World!</h1></html>"
        m.post("/bla", text=html)
        doc = context.fetch_html("https://test.com/bla", method="POST")
        assert doc.findtext(".//h1") == "Hello, World!"

    context.close()


def test_context_fetchers_exceptions(testdataset1: Dataset):
    context = Context(testdataset1)

    with pytest.raises(ValueError, match="Unsupported HTTP method.+"):
        context.fetch_text("https://test.com/bla", cache_days=0, method="PLOP")

    with pytest.raises(orjson.JSONDecodeError, match="unexpected.+"):
        with requests_mock.Mocker() as m:
            m.get("/bla", text='{"msg": "Hello, World!"')
            context.fetch_json("https://test.com/bla", cache_days=10)

    fingerprint = request_hash("https://test.com/bla", method="GET")

    # Checking that cleanup function wiped the cache properly
    assert context.cache.get(fingerprint, max_age=10) is None

    context.close()


def test_crawl_dataset(testdataset1: Dataset):
    DatasetSink(testdataset1).clear()
    assert len(list(iter_dataset_statements(testdataset1))) == 0
    context = Context(testdataset1)
    context.begin(clear=True)
    assert len(context.resources.all()) == 0
    func = load_entry_point(testdataset1)
    func(context)
    assert context.stats.entities > 5, context.stats.entities
    assert (
        context.stats.statements > context.stats.entities * 2
    ), context.stats.statements
    assert len(context.resources.all()) == 1
    context.close()
    assert len(list(iter_dataset_statements(testdataset1))) == context.stats.statements


def test_crawl_dataset_wrapper(testdataset1: Dataset):
    stats = crawl_dataset(testdataset1)
    assert stats.entities > 10

    testdataset1.disabled = True
    stats = crawl_dataset(testdataset1)
    assert stats.entities == 0

    testdataset1.disabled = False
    testdataset1.data.format = "FAIL"
    with pytest.raises(RunFailedException):
        crawl_dataset(testdataset1)
