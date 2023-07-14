import yaml
from typing import Optional, Dict, Any
from pathlib import Path
from nomenklatura.dataset import DataCatalog

from zavod.meta.dataset import Dataset
from zavod.archive import get_dataset_index


class ArchiveBackedCatalog(DataCatalog[Dataset]):
    def __init__(self) -> None:
        super().__init__(Dataset, {})

    def load_yaml(self, path: Path) -> Optional[Dataset]:
        with open(path, "r") as fh:
            data = yaml.safe_load(fh)
        if "name" not in data:
            data["name"] = path.stem
        dataset = Dataset(self, data)
        if dataset is not None:
            self.add(dataset)
        return dataset

    def get(self, name: str) -> Optional[Dataset]:
        dataset = super().get(name)
        if dataset is not None:
            return dataset
        path = get_dataset_index(name)
        if path is not None:
            return self.load_yaml(path)
        return None

    def to_opensanctions_dict(self) -> Dict[str, Any]:
        return {
            "datasets": [d.to_opensanctions_dict() for d in self.datasets],
            "updated_at": self.updated_at,
        }
