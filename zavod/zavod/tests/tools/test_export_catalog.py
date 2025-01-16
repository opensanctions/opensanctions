from json import load

from zavod.integration.dedupe import get_dataset_linker
from zavod.meta import get_catalog, load_dataset_from_path, Dataset
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


def test_export_index(testdataset1: Dataset, testdataset2: Dataset):
    # Create dataset index files
    crawl_dataset(testdataset1)
    export(testdataset1)

    crawl_dataset(testdataset2)
    export(testdataset2)

    # Clear catalog as if this is a fresh process separate from the earlier exports
    get_catalog.cache_clear()

    collection = load_dataset_from_path(COLLECTION_YML)
    assert collection is not None
    export(collection)
    export_index(collection)

    with open(settings.DATA_PATH / "datasets" / "index.json") as index_file:
        index = load(index_file)
        assert "datasets" in index
        assert "run_version" not in index
        assert "run_time" in index
        datasets = {r["name"] for r in index["datasets"]}
        assert "testdataset1" in datasets
        assert "testdataset2" in datasets
