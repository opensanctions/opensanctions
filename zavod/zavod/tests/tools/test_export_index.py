from zavod.meta import get_catalog, load_dataset_from_path, Dataset
from zavod.exporters.metadata import write_dataset_index
from zavod.tools.meta_index import export_index
from ..conftest import FIXTURES_PATH
from zavod.context import Context
from zavod import settings
from json import load


def test_export_index(vdataset: Dataset):
    # Create dataset index files
    context1 = Context(vdataset)
    write_dataset_index(context1, vdataset)
    dataset2 = load_dataset_from_path(FIXTURES_PATH / "test_dataset_2" / "test_dataset_2.yml")
    context2 = Context(dataset2)
    write_dataset_index(context2, dataset2)

    # Clear catalog as if this is a fresh process separate from the earlier exports
    get_catalog.cache_clear()

    collection = load_dataset_from_path(FIXTURES_PATH / "collection.yml")
    export_index(collection)

    with open(settings.DATA_PATH / "datasets" / "index.json") as index_file:
        index = load(index_file)
        datasets = {r["name"] for r in index["datasets"]}
        assert "validation" in datasets
        assert "test_dataset_2" in datasets
