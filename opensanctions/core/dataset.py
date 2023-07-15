from typing import Any, Dict
from urllib.parse import urljoin
from datapatch import get_lookups
from nomenklatura.dataset import DataCatalog
from zavod.dataset import ZavodDataset

from opensanctions import settings


class Dataset(ZavodDataset):
    """A dataset is a unit of execution of crawlers, and a grouping of entities.
    There are two types: sources (which relate to a specific crawlers), and
    collections (which group sources into more useful units)."""

    ALL = "all"
    DEFAULT = "default"
    TYPE = "default"

    def __init__(self, catalog: DataCatalog, type_: str, config: Dict[str, Any]):
        self.type = type_
        super().__init__(catalog, config)
        self.hidden: bool = config.get("hidden", False)
        self.export: bool = config.get("export", True)
        self.lookups = get_lookups(config.get("lookups", {}))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "type": self.type,
            "title": self.title,
            "hidden": self.hidden,
            "export": self.export,
            "summary": self.summary,
            "description": self.description,
        }

    def make_public_url(self, path: str) -> str:
        """Generate a public URL for a file within the dataset context."""
        url = urljoin(settings.DATASET_URL, f"{self.name}/")
        return urljoin(url, path)
