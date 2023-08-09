import pytest
import requests_mock
from datetime import datetime

from zavod import settings
from zavod.context import Context
from zavod.meta import Dataset
from zavod.entity import Entity
from zavod.runner import run_dataset
from zavod.archive import iter_dataset_statements
from zavod.runtime.sink import DatasetSink
from zavod.exc import RunFailedException
from zavod.runtime.loader import load_entry_point
from zavod.tests.conftest import XML_DOC


def test_context_helpers(vdataset: Dataset):
    context = Context(vdataset)
    assert context.dataset == vdataset
    assert "docs.google.com" in context.data_url
    assert vdataset.name in repr(context)
    gen_id = "osv-d5fdc7f711d0d9fd15421102d272e475a236005c"
    assert context.make_id("john", "doe") == gen_id
    assert context.make_id("") is None
    assert context.make_slug("john", "doe") == "osv-john-doe"
    assert context.make_slug(None) is None

    entity = context.make("Person")
    assert isinstance(entity, Entity)
    assert entity.schema.name == "Person"
    assert entity.dataset == vdataset

    with pytest.raises(ValueError, match="Entity has no ID.+"):
        context.emit(entity)

    entity.id = "test-id"
    with pytest.raises(ValueError, match="Entity has no properties."):
        context.emit(entity)

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


def test_context_dry_run(vdataset: Dataset):
    context = Context(vdataset, dry_run=True)
    assert context.dataset == vdataset
    context.begin(clear=True)
    assert context.dry_run
    context.log.error("Test error")
    context.close()
    assert list(context.issues.all()) == []


def test_context_fetchers(vdataset: Dataset):
    context = Context(vdataset)

    with requests_mock.Mocker() as m:
        m.get("/bla", text="Hello, World!")
        text = context.fetch_text("https://test.com/bla", cache_days=14)
        assert text == "Hello, World!"

    text = context.fetch_text("https://test.com/bla", cache_days=14)
    assert text == "Hello, World!"

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

    context.close()


def test_run_dataset(vdataset: Dataset):
    DatasetSink(vdataset).clear()
    assert len(list(iter_dataset_statements(vdataset))) == 0
    context = Context(vdataset)
    context.begin(clear=True)
    assert len(context.resources.all()) == 0
    func = load_entry_point(vdataset)
    func(context)
    assert context.stats.entities > 5, context.stats.entities
    assert (
        context.stats.statements > context.stats.entities * 2
    ), context.stats.statements
    assert len(context.resources.all()) == 1
    context.close()
    assert len(list(iter_dataset_statements(vdataset))) == context.stats.statements


def test_run_dataset_wrapper(vdataset: Dataset):
    stats = run_dataset(vdataset)
    assert stats.entities > 10

    vdataset.disabled = True
    stats = run_dataset(vdataset)
    assert stats.entities == 0

    vdataset.disabled = False
    vdataset.data.format = "FAIL"
    with pytest.raises(RunFailedException):
        run_dataset(vdataset)
