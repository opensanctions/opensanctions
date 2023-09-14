import json
from zavod import settings
from zavod.meta import Dataset
from zavod.store import get_view
from zavod.exporters import export_dataset


def test_metadata_collection_export(collection: Dataset) -> None:
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
