from json import load

import pytest
from followthemoney.dataset import Version

from zavod.archive import publish_version_history
from zavod.integration.dedupe import get_dataset_linker
from zavod.meta import get_catalog, load_dataset_from_path, Dataset
from zavod.runtime.versions import make_version, set_last_successful_version
from zavod.tools.export_catalog import export_index
from zavod import settings
from zavod.tests.conftest import COLLECTION_YML
from zavod.crawl import crawl_dataset
from zavod.store import get_store
from zavod.exporters import export_dataset


def export(dataset: Dataset) -> None:
    linker = get_dataset_linker(dataset)
    store = get_store(dataset, linker)
    store.sync()
    view = store.view(dataset)
    export_dataset(dataset, view)


def make_collection(testdataset1: Dataset, testdataset2: Dataset) -> Dataset:
    """Export both test datasets and the collection which contains them."""
    crawl_dataset(testdataset1)
    export(testdataset1)

    crawl_dataset(testdataset2)
    export(testdataset2)

    # Clear catalog as if this is a fresh process separate from the earlier exports
    get_catalog.cache_clear()

    collection = load_dataset_from_path(COLLECTION_YML)
    assert collection is not None
    export(collection)
    return collection


def run_default(version: Version, successful: bool) -> None:
    """Record a run of the default collection, which the catalog links its bulk
    statements file to, and archive the version history as a run would."""
    default = get_catalog().make_dataset(
        {
            "name": "default",
            "title": "Default collection",
            "datasets": ["testdataset1"],
        }
    )
    make_version(default, version, append_new_version_to_history=True)
    if successful:
        set_last_successful_version(default, version)
    publish_version_history(default.name)


def test_export_index(testdataset1: Dataset, testdataset2: Dataset):
    collection = make_collection(testdataset1, testdataset2)
    successful = Version.from_string("20260101000000-aaa")
    run_default(successful, successful=True)
    # A later failed run doesn't become the version we link statements to:
    run_default(Version.from_string("20260102000000-bbb"), successful=False)

    export_index(collection)

    with open(settings.DATA_PATH / "datasets" / "index.json") as index_file:
        index = load(index_file)
        assert "datasets" in index
        assert "run_version" not in index
        assert "run_time" in index
        datasets = {r["name"] for r in index["datasets"]}
        assert "testdataset1" in datasets
        assert "testdataset2" in datasets
        assert index["statements_url"] == (
            "https://data.opensanctions.org/artifacts/default/"
            f"{successful.id}/statements.csv"
        )


def test_export_index_without_successful_default(
    testdataset1: Dataset, testdataset2: Dataset
):
    """The catalog can't be written until the default collection has a
    successful run to link its bulk statements file to."""
    collection = make_collection(testdataset1, testdataset2)
    run_default(Version.from_string("20260101000000-aaa"), successful=False)

    with pytest.raises(RuntimeError, match="default"):
        export_index(collection)
