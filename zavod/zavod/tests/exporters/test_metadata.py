import json
import logging

from nomenklatura import Resolver
from pytest import MonkeyPatch
from structlog.testing import capture_logs

from zavod import settings
from zavod.archive import STATISTICS_FILE, dataset_resource_path
from zavod.context import Context
from zavod.crawl import crawl_dataset
from zavod.entity import Entity
from zavod.exporters import export_dataset, metadata
from zavod.exporters.metadata import DatasetVersionResult, write_dataset_index
from zavod.meta import Dataset
from zavod.exporters.metadata.model import CatalogDatasetModel
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

    with open(index_path) as fh:
        index = json.load(fh)
        assert index["updated_at"] == settings.RUN_TIME_ISO
        assert len(index["resources"]) > 2
        # When resolve is false, the resolve key is exported with correct value
        assert testdataset1.model.resolve is False
        assert index["resolve"] is False, index
        # The written index conforms to the output contract zavod validates against.
        CatalogDatasetModel.model_validate(index)

    collection_path = settings.DATA_PATH / "datasets" / collection.name
    export_dataset(collection, view)
    assert collection_path.is_dir()

    with open(collection_path / "index.json") as fh:
        collection_index = json.load(fh)
        # When resolve is true, the resolve key is not exported.
        assert collection.model.resolve is True
        assert "resolve" not in collection_index

    catalog_path = collection_path / "catalog.json"
    assert catalog_path.is_file()

    with open(catalog_path) as fh:
        catalog = json.load(fh)

    assert catalog["updated_at"] == settings.RUN_TIME_ISO
    assert len(catalog["datasets"]) == len(collection.datasets)
    for ds in catalog["datasets"]:
        assert ds["updated_at"] == settings.RUN_TIME_ISO
        if ds["name"] in (collection.name, testdataset1.name):
            assert len(ds["resources"]) > 2


def test_metadata_collection_issue_count(
    collection: Dataset, logger: logging.Logger
) -> None:
    """Issues logged against a collection (e.g. assemble errors surfaced when its
    store is built during export) are counted in the collection's index.json."""
    context = Context(collection)
    context.begin(clear=True)
    context.log.error("This is an assemble error")
    context.log.warning("This is a warning")
    context.close()

    write_dataset_index(collection, DatasetVersionResult.SUCCESS)

    index_path = settings.DATA_PATH / "datasets" / collection.name / "index.json"
    with open(index_path) as fh:
        index = json.load(fh)
    assert index["issue_count"] == 2, index
    assert index["issue_levels"] == {"error": 1, "warning": 1}, index


def test_metadata_validation_warns_on_missing_required_field(
    collection: Dataset, monkeypatch: MonkeyPatch
) -> None:
    """A successful run whose metadata is missing a required field only warns;
    the index is still written."""
    context = Context(collection)
    context.begin(clear=True)
    context.close()

    with open(dataset_resource_path(collection.name, STATISTICS_FILE), "w") as fh:
        json.dump(
            {
                "entity_count": 5,
                "things": {"total": 5},
                "targets": {"total": 2},
                "last_change": settings.RUN_TIME_ISO,
            },
            fh,
        )

    real_get_base = metadata.get_base_dataset_metadata

    def drop_required_field(dataset: Dataset) -> dict:
        meta = real_get_base(dataset)
        del meta["entity_count"]
        return meta

    monkeypatch.setattr(metadata, "get_base_dataset_metadata", drop_required_field)

    with capture_logs() as cap_logs:
        write_dataset_index(collection, DatasetVersionResult.SUCCESS)

    assert any(
        entry.get("log_level") == "warning"
        and "catalog model" in entry.get("event", "")
        for entry in cap_logs
    )
    assert (settings.DATA_PATH / "datasets" / collection.name / "index.json").is_file()


def test_metadata_failure_no_statistics_no_warning(collection: Dataset) -> None:
    """A failed run legitimately lacks statistics, so the model tolerates the
    missing fields and validation does not warn."""
    context = Context(collection)
    context.begin(clear=True)
    context.close()

    assert not dataset_resource_path(collection.name, STATISTICS_FILE).is_file()
    with capture_logs() as cap_logs:
        write_dataset_index(collection, DatasetVersionResult.FAILURE)

    assert not [
        entry
        for entry in cap_logs
        if entry.get("log_level") == "warning"
        and "catalog model" in entry.get("event", "")
    ]

    index_path = settings.DATA_PATH / "datasets" / collection.name / "index.json"
    with open(index_path, "r") as fh:
        index = json.load(fh)
    assert index["result"] == "failure"
    assert "entity_count" not in index
