from typing import cast

import orjson
import pytest
import requests_mock
import structlog
from requests.adapters import HTTPAdapter
from followthemoney.statement import read_statements, PACK

from zavod import settings
from zavod.archive import iter_dataset_statements, dataset_resource_path
from zavod.archive import STATEMENTS_FILE
from zavod.context import Context
from zavod.crawl import crawl_dataset
from zavod.entity import Entity
from zavod.exc import RunFailedException
from zavod.meta import Dataset
from zavod.runtime.http_ import request_hash
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
    with structlog.testing.capture_logs() as caplogs:
        assert context.lookup_value("plants", "stone") is None
        assert context.lookup_value("plants", "rock", warn_unmatched=True) is None
    assert caplogs == [
        {
            "event": "No matching lookup found.",
            "log_level": "warning",
            "lookup": "plants",
            "value": "rock",
        }
    ]

    context.inspect(None)
    context.inspect("foo")

    # don't know how to assert anything here
    context.audit_data({"test": "bla", "foo": 3}, ignore=["foo"])
    context.close()


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

        # Testing caching
        text = context.fetch_text("https://test.com/bla", cache_days=14)
        assert text == "Hello, World!"

        assert m.call_count == 1

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

    with requests_mock.Mocker() as m:
        m.post("/bla", text=long)
        path = context.fetch_resource(
            "world-post.txt",
            "https://test.com/bla",
            method="POST",
            data={"foo": "bar"},
        )
    assert path.stem == "world-post"
    assert m.request_history[0].body == "foo=bar"
    with open(path, "r") as fh:
        assert fh.read() == long

    path = context.get_resource_path("doc.xml")
    with open(path, "w") as fh:
        with open(XML_DOC, "r") as src:
            fh.write(src.read())
    xml_doc = context.parse_resource_xml("doc.xml")
    assert "MyAddress" in xml_doc.getroot().tag

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

        assert m.call_count == 1

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

    params = {"query": "test"}
    data = {"foo": "bar"}
    auth = ("user", "pass")
    # Test that JSON decode failure clears its cache entry

    context.cache.clear()
    with pytest.raises(orjson.JSONDecodeError, match="unexpected.+"):
        with requests_mock.Mocker() as m:
            m.post("/bla", text='{"msg": "Jason who? The object doesn\'t close."')

            context.fetch_json(
                "https://test.com/bla",
                method="POST",
                auth=auth,
                params=params,
                data=data,
                cache_days=10,
            )

    # Checking that cleanup function wiped the cache properly
    assert list(context.cache.all(None)) == []

    # Test that HTML parse failure clears its cache entry
    with pytest.raises(ValueError):
        with requests_mock.Mocker() as m:
            m.post("/bla", text="")
            context.fetch_html(
                "https://test.com/bla",
                method="POST",
                auth=auth,
                params=params,
                data=data,
                cache_days=10,
            )

    # Checking that cleanup function wiped the cache properly
    assert list(context.cache.all(None)) == []

    context.close()


def test_crawl_dataset(testdataset1: Dataset):
    path = dataset_resource_path(testdataset1.name, STATEMENTS_FILE)
    if path.is_file():
        path.unlink()
    assert len(list(iter_dataset_statements(testdataset1))) == 0
    context = Context(testdataset1)
    context.begin(clear=True)
    assert len(context.resources.all()) == 0
    func = load_entry_point(testdataset1)
    func(context)
    assert context.stats.entities > 5, context.stats.entities
    assert context.stats.statements > context.stats.entities * 2, (
        context.stats.statements
    )
    assert len(context.resources.all()) == 1
    context.close()
    assert len(list(iter_dataset_statements(testdataset1))) == context.stats.statements


def test_crawl_dataset_wrapper(testdataset1: Dataset):
    stats = crawl_dataset(testdataset1)
    assert stats.entities > 10

    testdataset1.model.disabled = True
    stats = crawl_dataset(testdataset1)
    assert stats.entities == 0

    testdataset1.model.disabled = False
    assert testdataset1.data is not None
    testdataset1.data.format = "FAIL"
    with pytest.raises(RunFailedException):
        crawl_dataset(testdataset1)


def test_dataset_sink(testdataset1: Dataset):
    context = Context(testdataset1)
    assert context._writer_path.is_relative_to(settings.DATA_PATH)
    entity = context.make("Person")
    entity.id = "foo"
    entity.add("name", "Foo")
    context.emit(entity)
    context.close()
    assert context._writer_path.is_file()
    with open(context._writer_path, "rb") as fh:
        stmts = list(read_statements(fh, PACK))
        for stmt in stmts:
            assert stmt.dataset == testdataset1.name, stmt
            assert stmt.entity_id == "foo"
        assert len(stmts) == 2, stmts
        props = [s.prop for s in stmts]
        assert "id" in props, props
        assert "name" in props, props
    # context.sink.clear()
    # assert not context._writer_path.is_file()
