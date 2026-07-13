import yaml
from typing import Optional
from pathlib import Path
from followthemoney.dataset import DataCatalog

from zavod.meta.dataset import Dataset
from zavod.archive import find_archive_artifact, get_dataset_artifact
from zavod.archive import latest_local_version, INDEX_FILE


class ArchiveBackedCatalog(DataCatalog[Dataset]):
    def __init__(self) -> None:
        super().__init__(Dataset, {})

    def load_yaml(self, path: Path) -> Optional[Dataset]:
        with open(path, "r") as fh:
            data = yaml.safe_load(fh)
        if "name" not in data:
            data["name"] = path.stem
        dataset = Dataset(data)
        dataset.base_path = path.parent
        self.add(dataset)
        for name in dataset.model.children:
            self.get(name)
        return dataset

    def get(self, name: str) -> Optional[Dataset]:
        dataset = super().get(name)
        if dataset is not None:
            return dataset
        # The dataset is not defined locally, so use the metadata of the newest run
        # available: a local one if present, otherwise from the archive.
        version = latest_local_version(name, with_resource=INDEX_FILE)
        if version is None:
            version, _ = find_archive_artifact(name, INDEX_FILE)
        if version is None:
            return None
        path = get_dataset_artifact(name, version, INDEX_FILE)
        if path.exists():
            return self.load_yaml(path)
        return None
