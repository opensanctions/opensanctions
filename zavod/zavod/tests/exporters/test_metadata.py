import json
import logging

from followthemoney.dataset import Version
from nomenklatura import Resolver
from pytest import MonkeyPatch
from structlog.testing import capture_logs

from zavod import settings
from zavod.archive import STATISTICS_FILE, dataset_artifact_directory
from zavod.archive import dataset_artifact_path
from zavod.crawl import crawl_dataset
from zavod.entity import Entity
from zavod.exporters import export_dataset, metadata
from zavod.exporters.metadata import DatasetVersionResult, write_dataset_index
from zavod.meta import Dataset
from zavod.exporters.metadata.model import CatalogDatasetModel
from zavod.publish import publish_dataset
from zavod.tests.util import get_manifest, get_test_view, make_context


def test_metadata_collection_export(
    testdataset1: Dataset, collection: Dataset, resolver: Resolver[Entity]
) -> None:
    version = settings.RUN_VERSION
    ds_path = dataset_artifact_directory(testdataset1.name, version)
    crawl_dataset(testdataset1, version)
    view = get_test_view(testdataset1, linker=resolver)
    export_dataset(testdataset1, version, view)
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

    # In a production run, sources are published before a collection exports;
    # the catalog takes each dataset's run information from its last published
    # version.
    publish_dataset(testdataset1, version)

    collection_path = dataset_artifact_directory(collection.name, version)
    get_manifest(collection, version)
    export_dataset(collection, version, view)
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
        if ds["name"] == testdataset1.name:
            assert len(ds["resources"]) > 2
        if ds["name"] == collection.name:
            # The collection itself has no published version yet, so its own
            # catalog entry carries no run information.
            assert "resources" not in ds


def test_metadata_collection_issue_count(
    collection: Dataset, logger: logging.Logger
) -> None:
    """Issues logged against a collection (e.g. assemble errors surfaced when its
    store is built during export) are counted in the collection's index.json."""
    context = make_context(collection)
    context.begin()
    context.log.error("This is an assemble error")
    context.log.warning("This is a warning")
    context.close()

    write_dataset_index(collection, context.version, DatasetVersionResult.SUCCESS)

    index_path = dataset_artifact_path(collection.name, context.version, "index.json")
    with open(index_path) as fh:
        index = json.load(fh)
    assert index["issue_count"] == 2, index
    assert index["issue_levels"] == {"error": 1, "warning": 1}, index


def test_metadata_validation_warns_on_missing_required_field(
    collection: Dataset, monkeypatch: MonkeyPatch
) -> None:
    """A successful run whose metadata is missing a required field only warns;
    the index is still written."""
    context = make_context(collection)
    context.begin()
    context.close()
    version = context.version

    statistics_path = dataset_artifact_path(collection.name, version, STATISTICS_FILE)
    with open(statistics_path, "w") as fh:
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

    def drop_required_field(
        dataset: Dataset, version: Version, result: DatasetVersionResult
    ) -> dict:
        meta = real_get_base(dataset, version, result)
        del meta["entity_count"]
        return meta

    monkeypatch.setattr(metadata, "get_base_dataset_metadata", drop_required_field)

    with capture_logs() as cap_logs:
        write_dataset_index(collection, version, DatasetVersionResult.SUCCESS)

    assert any(
        entry.get("log_level") == "warning"
        and "catalog model" in entry.get("event", "")
        for entry in cap_logs
    )
    assert dataset_artifact_path(collection.name, version, "index.json").is_file()


def test_metadata_failure_no_statistics_no_warning(collection: Dataset) -> None:
    """A failed run legitimately lacks statistics, so the model tolerates the
    missing fields and validation does not warn."""
    context = make_context(collection)
    context.begin()
    context.close()
    version = context.version

    statistics_path = dataset_artifact_path(collection.name, version, STATISTICS_FILE)
    assert not statistics_path.is_file()
    with capture_logs() as cap_logs:
        write_dataset_index(collection, version, DatasetVersionResult.FAILURE)

    assert not [
        entry
        for entry in cap_logs
        if entry.get("log_level") == "warning"
        and "catalog model" in entry.get("event", "")
    ]

    index_path = dataset_artifact_path(collection.name, version, "index.json")
    with open(index_path) as fh:
        index = json.load(fh)
    assert index["result"] == "failure"
    assert "entity_count" not in index
