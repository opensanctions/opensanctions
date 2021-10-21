from typing import Any, Dict, Set, cast
from opensanctions.core.dataset import Dataset
from opensanctions.core.source import Source


class Collection(Dataset):
    """A grouping of individual data sources. Data sources are bundled in order
    to be more useful for list use."""

    TYPE = "collection"

    def __init__(self, file_path, config):
        super().__init__(self.TYPE, file_path, config)

    @property
    def datasets(self) -> Set[Dataset]:
        datasets: Set[Dataset] = set([self])
        for dataset in Dataset.all():
            if self.name in dataset.collections:
                datasets.update(dataset.datasets)
        return datasets

    @property
    def sources(self) -> Set[Source]:
        return set([cast(Source, t) for t in self.datasets if t.TYPE == Source.TYPE])

    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data["sources"] = [s.name for s in self.sources]
        return data
