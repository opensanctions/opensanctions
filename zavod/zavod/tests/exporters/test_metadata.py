import json
from zavod import settings
from zavod.meta import Dataset
from zavod.store import get_view
from zavod.crawl import crawl_dataset
from zavod.exporters import export_dataset


def test_metadata_collection_export(testdataset1: Dataset, collection: Dataset) -> None:
    ds_path = settings.DATA_PATH / "datasets" / testdataset1.name
    crawl_dataset(testdataset1)
    view = get_view(testdataset1)
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

    collection_path = settings.DATA_PATH / "datasets" / collection.name
    view = get_view(collection)
    export_dataset(collection, view)
    assert collection_path.is_dir()
    catalog_path = collection_path / "catalog.json"
    assert catalog_path.is_file()

    with open(catalog_path, "r") as fh:
        catalog = json.load(fh)

    assert catalog["updated_at"] == settings.RUN_TIME_ISO
    assert len(catalog["datasets"]) == len(collection.datasets)
    for ds in catalog["datasets"]:
        assert ds["updated_at"] == settings.RUN_TIME_ISO
        assert len(ds["resources"]) > 2
