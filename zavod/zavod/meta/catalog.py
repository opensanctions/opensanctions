import yaml
from pathlib import Path
from followthemoney.dataset import DataCatalog

from zavod.meta.dataset import Dataset
from zavod.archive import get_dataset_artifact, INDEX_FILE


class ArchiveBackedCatalog(DataCatalog[Dataset]):
    def __init__(self) -> None:
        super().__init__(Dataset, {})

    def load_yaml(self, path: Path, traverse_children: bool = True) -> Dataset | None:
        with open(path) as fh:
            data = yaml.safe_load(fh)
        if "name" not in data:
            data["name"] = path.stem
        dataset = Dataset(data)
        dataset.base_path = path.parent
        self.add(dataset)
        if traverse_children:
            # Resolving a child via get() may download its index.json from the
            # archive. Disable when the caller loads the whole dataset tree
            # anyway (cf. load_directory_catalog); add() links children in any order.
            for name in dataset.model.children:
                self.get(name)
        return dataset

    def get(self, name: str) -> Dataset | None:
        dataset = super().get(name)
        if dataset is not None:
            return dataset
        path = get_dataset_artifact(name, INDEX_FILE)
        if path.exists():
            return self.load_yaml(path)
        return None
