from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set
from banal import ensure_list
from urllib.parse import urljoin
from datapatch import get_lookups
from functools import cached_property
from followthemoney.types import registry
from nomenklatura.dataset import Dataset as NomenklaturaDataset

from opensanctions import settings
from opensanctions.core.lookups import load_yaml
from opensanctions.core.db import KEY_LEN

if TYPE_CHECKING:
    from opensanctions.core.source import Source


class DatasetPublisher(object):
    """Publisher information, eg. the government authority."""

    def __init__(self, config):
        self.url = config.get("url")
        self.name = config.get("name")
        self.description = config.get("description")
        self.country = config.get("country", "zz")
        assert registry.country.validate(self.country), "Invalid publisher country"

    def to_dict(self):
        return {
            "url": self.url,
            "name": self.name,
            "description": self.description,
            "country": self.country,
            "country_label": registry.country.caption(self.country),
        }


class Dataset(NomenklaturaDataset):
    """A dataset is a unit of execution of crawlers, and a grouping of entities.
    There are two types: sources (which relate to a specific crawlers), and
    collections (which group sources into more useful units)."""

    ALL = "all"
    DEFAULT = "default"
    TYPE = "default"

    def __init__(self, type_, file_path, config):
        self.type = type_
        self.file_path = file_path
        name = config.get("name", file_path.stem)
        title = config.get("title", name)
        super().__init__(name, title)
        self.prefix = config.get("prefix", self.name)
        self.hidden = config.get("hidden", False)
        self.summary = config.get("summary", "")
        self.description = config.get("description", "")

        # Collections can be part of other collections.
        collections = ensure_list(config.get("collections"))
        if self.name != self.ALL:
            collections.append(self.ALL)
        self.collections = set(collections)

        self.lookups = get_lookups(config.get("lookups", {}))

    @cached_property
    def datasets(self) -> Set["Dataset"]:
        return set([self])

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
    def _from_metadata(cls, file_path):
        from opensanctions.core.source import Source
        from opensanctions.core.external import External
        from opensanctions.core.collection import Collection

        config = load_yaml(file_path)
        type_ = config.get("type", Source.TYPE)
        type_ = type_.lower().strip()
        if type_ == Collection.TYPE:
            return Collection(file_path, config)
        if type_ == Source.TYPE:
            return Source(file_path, config)
        if type_ == External.TYPE:
            return External(file_path, config)

    @classmethod
    def _load_cache(cls):
        if not hasattr(cls, "_cache"):
            cls._cache: Dict[str, Dataset] = {}
            for glob in ("**/*.yml", "**/*.yaml"):
                for file_path in settings.METADATA_PATH.glob(glob):
                    dataset = cls._from_metadata(file_path)
                    if dataset is not None:
                        cls._cache[dataset.name] = dataset
        return cls._cache

    @classmethod
    def all(cls) -> List["Dataset"]:
        return sorted(cls._load_cache().values())

    @classmethod
    def get(cls, name) -> Optional["Dataset"]:
        return cls._load_cache().get(name)

    @classmethod
    def require(cls, name) -> "Dataset":
        dataset = cls.get(name)
        if dataset is None:
            raise ValueError("No such dataset: %s" % name)
        return dataset

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
            "summary": self.summary,
            "description": self.description,
        }

    def make_public_url(self, path: str) -> str:
        """Generate a public URL for a file within the dataset context."""
        url = urljoin(settings.DATASET_URL, f"{self.name}/")
        return urljoin(url, path)
