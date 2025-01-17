import json

from nomenklatura import Resolver

from zavod import settings
from zavod.crawl import crawl_dataset
from zavod.entity import Entity
from zavod.exporters import export_dataset
from zavod.integration import get_resolver
from zavod.meta import Dataset
from zavod.store import get_store


def test_metadata_collection_export(
    testdataset1: Dataset, collection: Dataset, resolver: Resolver[Entity]
) -> None:
    ds_path = settings.DATA_PATH / "datasets" / testdataset1.name
    crawl_dataset(testdataset1)
    store = get_store(testdataset1, resolver)
    store.sync()
    view = store.view(testdataset1)
    export_dataset(testdataset1, view)
    assert ds_path.is_dir()
    catalog_path = ds_path / "catalog.json"
    assert not catalog_path.is_file()
    index_path = ds_path / "index.json"
    assert index_path.is_file()

    with open(index_path, "r") as fh:
        index = json.load(fh)
        assert index["updated_at"] == settings.RUN_TIME_ISO
        assert len(index["resources"]) > 2
        # When resolve is false, the resolve key is exported with correct value
        assert testdataset1.resolve is False
        assert index["resolve"] is False, index

    collection_path = settings.DATA_PATH / "datasets" / collection.name
    export_dataset(collection, view)
    assert collection_path.is_dir()

    with open(collection_path / "index.json", "r") as fh:
        collection_index = json.load(fh)
        # When resolve is true, the resolve key is not exported.
        assert collection.resolve is True
        assert "resolve" not in collection_index

    catalog_path = collection_path / "catalog.json"
    assert catalog_path.is_file()

    with open(catalog_path, "r") as fh:
        catalog = json.load(fh)

    assert catalog["updated_at"] == settings.RUN_TIME_ISO
    assert len(catalog["datasets"]) == len(collection.datasets)
    for ds in catalog["datasets"]:
        assert ds["updated_at"] == settings.RUN_TIME_ISO
        if ds["name"] in (collection.name, testdataset1.name):
            assert len(ds["resources"]) > 2
