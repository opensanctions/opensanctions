import pytest
from nomenklatura.exceptions import MetadataException
from zavod import settings

from zavod.meta import get_catalog, Dataset, get_multi_dataset
from zavod.meta.assertion import Assertion
from zavod.runtime.urls import make_published_url


TEST_DATASET = {
    "name": "test",
    "title": "Test Dataset",
    "hidden": True,
    "prefix": "xx",
    "resolve": False,
    "data": {
        "url": "https://example.com/data.csv",
        "format": "csv",
    },
    "http": {
        "total_retries": 1,
        "backoff_factor": 0.5,
        "retry_statuses": [500],
        "retry_methods": ["GET"],
    },
}

TEST_COLLECTION = {
    "name": "collection",
    "title": "Test Collection",
    "datasets": ["test"],
}


def test_basic():
    catalog = get_catalog()
    test_ds = catalog.make_dataset(TEST_DATASET)
    coll_ds = catalog.make_dataset(TEST_COLLECTION)
    assert len(catalog.datasets) == 2
    assert catalog.has("test") is True
    assert catalog.require("test") == test_ds
    assert catalog.has("testX") is False
    with pytest.raises(MetadataException):
        catalog.require("testX")

    assert test_ds.hidden is True
    assert test_ds.prefix == "xx"
    assert test_ds.is_collection is False
    assert test_ds.data.url is not None
    assert test_ds.disabled is False
    assert not len(test_ds.inputs)
    url = make_published_url(test_ds.name, "foo.json")
    assert url.startswith("https://data.opensanctions.org/datasets/"), url
    assert url.endswith(f"{test_ds.name}/foo.json"), url
    os_data = test_ds.to_opensanctions_dict(catalog)
    assert os_data["name"] == "test", os_data
    assert os_data["collections"] == ["collection"], os_data
    assert test_ds.resolve is False
    assert os_data["resolve"] is False, os_data
    # Explicit True is also read correctly
    resolve_meta = TEST_DATASET.copy()
    resolve_meta["resolve"] = True
    resolve_ds = catalog.make_dataset(resolve_meta)
    assert resolve_ds.resolve is True

    assert coll_ds.hidden is False
    assert coll_ds.is_collection is True
    assert len(coll_ds.children) == 1
    assert coll_ds.data is None
    os_data = coll_ds.to_opensanctions_dict(catalog)
    assert "collections" not in os_data, os_data
    assert os_data["datasets"] == ["test"], os_data
    # When resolve isn't set in the metadata, it defaults to True.
    assert "resolve" not in TEST_COLLECTION
    assert coll_ds.resolve is True
    # When it's true, it isn't dumped.
    assert "resolve" not in os_data, os_data

    assert test_ds.http.total_retries == 1
    assert test_ds.http.retry_statuses == [500]
    assert test_ds.http.retry_methods == ["GET"]
    assert test_ds.http.backoff_factor == 0.5
    assert test_ds.http.user_agent == settings.HTTP_USER_AGENT


def test_validation(testdataset1: Dataset, testdataset3: Dataset):
    assert testdataset1.name == "testdataset1"
    assert testdataset1.publisher is not None
    assert testdataset1.publisher.name == "OpenSanctions"
    assert testdataset1.publisher.official is False
    assert len(testdataset1.children) == 0
    assert len(testdataset1.datasets) == 1
    assert len(testdataset1.inputs) == 0
    assert len(testdataset1.assertions) == 0
    assert len(testdataset3.assertions) == 5
    assert isinstance(testdataset3.assertions[0], Assertion)


def test_validation_os_dict(testdataset1: Dataset, collection: Dataset):
    catalog = get_catalog()
    osa = testdataset1.to_opensanctions_dict(catalog)
    assert osa["name"] == "testdataset1"
    assert osa["type"] == "source"
    assert osa["publisher"]["name"] == "OpenSanctions"
    assert osa["publisher"]["official"] is False
    assert osa["data"]["url"] is not None
    assert osa["data"]["format"] == "CSV"
    assert "hidden" in osa
    assert "exports" not in osa
    assert "summary" in osa
    assert "description" in osa
    assert osa["entry_point"] == "testentrypoint1"

    osac = collection.to_opensanctions_dict(catalog)
    assert osac["name"] == "collection"
    assert osac["type"] == "collection"
    assert len(osac["datasets"]) == 1, osac["datasets"]
    assert len(osac["children"]) == 2, osac["children"]
    assert "sources" not in osac
    # assert len(osac["sources"]) == 1
    # assert len(osac["externals"]) == 0
    assert "entry_point" not in osac


def test_analyzer(analyzer: Dataset, testdataset1: Dataset):
    assert len(analyzer.inputs) == 1
    assert len(analyzer.inputs)


def test_multi_dataset(analyzer: Dataset, testdataset1: Dataset):
    with pytest.raises(MetadataException):
        get_multi_dataset(["xxxx"])

    ds = get_multi_dataset([analyzer.name])
    assert ds == analyzer

    ds = get_multi_dataset([analyzer.name, testdataset1.name])
    assert analyzer in ds.children
    assert testdataset1 in ds.children
