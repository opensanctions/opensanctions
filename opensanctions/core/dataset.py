from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set
from banal import ensure_list
from urllib.parse import urljoin
from datapatch import get_lookups
from functools import cached_property
from nomenklatura.dataset import Dataset as NomenklaturaDataset
from nomenklatura.dataset import DataCatalog

from opensanctions import settings
from opensanctions.core.lookups import load_yaml

if TYPE_CHECKING:
    from opensanctions.core.source import Source


class Dataset(NomenklaturaDataset):
    """A dataset is a unit of execution of crawlers, and a grouping of entities.
    There are two types: sources (which relate to a specific crawlers), and
    collections (which group sources into more useful units)."""

    ALL = "all"
    DEFAULT = "default"
    TYPE = "default"

    def __init__(self, catalog: DataCatalog, type_: str, config: Dict[str, Any]):
        self.type = type_
        super().__init__(catalog, config)
        self.prefix: str = config.get("prefix", self.name)
        self.hidden: bool = config.get("hidden", False)
        self.export: bool = config.get("export", True)
        if self.name != self.ALL:
            self._parents.add(self.ALL)
        self.lookups = get_lookups(config.get("lookups", {}))

    @cached_property
    def scopes(self) -> Set["Dataset"]:
        return set([self])

    @cached_property
    def sources(self) -> Set["Source"]:
        return set()

    @property
    def source_names(self) -> List[str]:
        return [s.name for s in self.sources]

    @property
    def scope_names(self) -> List[str]:
        return [s.name for s in self.scopes]

    def provided_datasets(self) -> List["Dataset"]:
        """Return a list of datasets which are in the sources or can be built from
        the same sources. Basically: all datasets that are smaller in scope than
        this one."""
        datasets: List[Dataset] = []
        available = set(self.scope_names)
        for dataset in Dataset.all():
            required = set(dataset.scope_names)
            matches = available.intersection(required)
            if len(matches) == len(required):
                datasets.append(dataset)
        return datasets

    @classmethod
    def _from_metadata(cls, catalog, file_path):
        from opensanctions.core.source import Source
        from opensanctions.core.external import External
        from opensanctions.core.collection import Collection

        config: Dict[str, Any] = load_yaml(file_path)
        if "name" not in config:
            config["name"] = file_path.stem
        type_: str = config.get("type", Source.TYPE)
        type_ = type_.lower().strip()
        if type_ == Collection.TYPE:
            return Collection(catalog, config)
        if type_ == Source.TYPE:
            return Source(catalog, config)
        if type_ == External.TYPE:
            return External(catalog, config)

    @classmethod
    def _load_catalog(cls) -> DataCatalog:
        if not len(CATALOG.datasets):
            for glob in ("**/*.yml", "**/*.yaml"):
                for file_path in settings.METADATA_PATH.glob(glob):
                    dataset = cls._from_metadata(CATALOG, file_path)
                    if dataset is not None:
                        CATALOG.add(dataset)
        return CATALOG

    @classmethod
    def all(cls) -> List["Dataset"]:
        return sorted(cls._load_catalog().datasets)

    @classmethod
    def get(cls, name) -> Optional["Dataset"]:
        return cls._load_catalog().get(name)

    @classmethod
    def require(cls, name) -> "Dataset":
        return cls._load_catalog().require(name)

    @classmethod
    def names(cls) -> List[str]:
        """An array of all dataset names found in the metadata path."""
        return [dataset.name for dataset in cls.all()]

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


CATALOG = DataCatalog(Dataset, {})
