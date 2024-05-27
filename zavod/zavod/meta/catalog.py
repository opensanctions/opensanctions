import yaml
from typing import Optional
from pathlib import Path
from nomenklatura.dataset import DataCatalog

from zavod.meta.dataset import Dataset
from zavod.archive import get_dataset_artifact, INDEX_FILE


class ArchiveBackedCatalog(DataCatalog[Dataset]):
    def __init__(self) -> None:
        super().__init__(Dataset, {})

    def load_yaml(self, path: Path) -> Optional[Dataset]:
        with open(path, "r") as fh:
            data = yaml.safe_load(fh)
        if "name" not in data:
            data["name"] = path.stem
        dataset = Dataset(self, data)
        dataset.base_path = path.parent
        if dataset is not None:
            self.add(dataset)
        return dataset

    def get(self, name: str) -> Optional[Dataset]:
        dataset = super().get(name)
        if dataset is not None:
            return dataset
        path = get_dataset_artifact(name, INDEX_FILE)
        if path.exists():
            return self.load_yaml(path)
        return None
