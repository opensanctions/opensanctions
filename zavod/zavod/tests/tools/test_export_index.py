from json import load

from zavod.meta import get_catalog, load_dataset_from_path, Dataset
from zavod.tools.meta_index import export_index
from zavod import settings
from ..conftest import FIXTURES_PATH
from zavod.crawl import crawl_dataset
from zavod.exporters import export

COLLECTION_YML = FIXTURES_PATH / "collection.yml"


def test_export_index(testdataset1: Dataset, testdataset2: Dataset):
    # Create dataset index files
    crawl_dataset(testdataset1)
    export(testdataset1.name)

    crawl_dataset(testdataset2)
    export(testdataset2.name)

    # Clear catalog as if this is a fresh process separate from the earlier exports
    get_catalog.cache_clear()

    collection = load_dataset_from_path(COLLECTION_YML)
    assert collection is not None
    export(collection.name)
    export_index(collection)

    with open(settings.DATA_PATH / "datasets" / "index.json") as index_file:
        index = load(index_file)
        datasets = {r["name"] for r in index["datasets"]}
        assert "testdataset1" in datasets
        assert "testdataset2" in datasets
