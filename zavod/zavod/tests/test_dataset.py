import pytest
from zavod.meta import get_catalog, Dataset

from nomenklatura.exceptions import MetadataException

TEST_DATASET = {
    "name": "test",
    "title": "Test Dataset",
    "hidden": True,
    "prefix": "xx",
    "data": {
        "url": "https://example.com/data.csv",
        "format": "csv",
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
    assert test_ds.input is None
    url = test_ds.make_public_url("foo")
    assert url.startswith("https://data.opensanctions.org/"), url
    assert url.endswith("/foo"), url
    os_data = test_ds.to_opensanctions_dict()
    assert os_data["name"] == "test", os_data
    assert os_data["collections"] == ["collection"], os_data

    assert coll_ds.hidden is False
    assert coll_ds.is_collection is True
    assert len(coll_ds.children) == 1
    assert coll_ds.data is None
    os_data = coll_ds.to_opensanctions_dict()
    assert "collections" not in os_data, os_data
    assert os_data["sources"] == ["test"], os_data


def test_validation(vdataset: Dataset):
    assert vdataset.name == "validation"
    assert vdataset.publisher is not None
    assert vdataset.publisher.name == "OpenSanctions"
    assert vdataset.publisher.official is False
    assert len(vdataset.children) == 0
    assert len(vdataset.datasets) == 1
    assert vdataset.input is None


def test_validation_os_dict(vdataset: Dataset):
    osa = vdataset.to_opensanctions_dict()
    assert osa["name"] == "validation"
    assert osa["publisher"]["name"] == "OpenSanctions"
    assert osa["publisher"]["official"] is False
    assert osa["data"]["url"] is not None
    assert osa["data"]["format"] == "CSV"
    assert "hidden" in osa
    assert "export" in osa
    assert "summary" in osa
    assert "description" in osa
    assert osa["entry_point"] == "validation"


def test_analyzer(analyzer: Dataset, vdataset: Dataset):
    assert analyzer.input is not None
    assert analyzer.input == vdataset
