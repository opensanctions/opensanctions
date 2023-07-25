from ..exporters import export
from zavod.context import Context
from zavod.store import View, get_store, get_view
from zavod.meta import Dataset
from zavod.runner import run_dataset
from zavod import settings


def test_export(vdataset: Dataset):
    stats = run_dataset(vdataset)
    export(vdataset.name)

    dataset_path = settings.DATA_PATH / "datasets" / vdataset.name
    open(dataset_path / "entities.ftm.json").close()
    open(dataset_path / "index.json").close()
    open(dataset_path / "names.txt").close()
    open(dataset_path / "resources.json").close()
    open(dataset_path / "senzing.json").close()
    open(dataset_path / "statistics.json").close()
    open(dataset_path / "targets.nested.json").close()
    open(dataset_path / "targets.simple.csv").close()