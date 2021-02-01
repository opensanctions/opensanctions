import os
import yaml
from importlib import import_module
from ftmstore import get_dataset as get_store

from opensanctions import settings


class DatasetData(object):
    """Data source specification."""

    def __init__(self, config):
        self.url = config.get("url")
        self.mode = config.get("mode")
        self.format = config.get("format")
        self.api_key = config.get("api_key")
        if self.api_key is not None:
            self.api_key = os.path.expandvars(self.api_key)

    def to_dict(self):
        return {"url": self.url, "fomat": self.format, "mode": self.mode}


class DatasetPublisher(object):
    """Publisher information, eg. the government authority."""

    def __init__(self, config):
        self.url = config.get("url")
        self.title = config.get("title")

    def to_dict(self):
        return {"url": self.url, "title": self.title}


class Dataset(object):
    """A dataset to be included in OpenSanctions, backed by a crawler
    that can acquire and transform the data.
    """

    def __init__(self, file_path, config):
        self.file_path = file_path
        self.name = file_path.stem
        self.url = config.get("url", "")
        self.title = config.get("title", self.name)
        self.country = config.get("country", "zz")
        self.category = config.get("category", "other")
        self.description = config.get("description", "")
        self.entry_point = config.get("entry_point")
        self.data = DatasetData(config.get("data", {}))
        self.publisher = DatasetPublisher(config.get("publisher", {}))

    @property
    def store(self):
        name = f"dataset_{self.name}"
        return get_store(name, database_uri=settings.DATABASE_URI)

    @property
    def method(self):
        """Load the actual crawler code behind the dataset."""
        method = "crawl"
        package = self.entry_point
        if package is None:
            raise RuntimeError("The dataset has no entry point!")
        if ":" in package:
            package, method = package.rsplit(":", 1)
        module = import_module(package)
        return getattr(module, method)

    def to_dict(self):
        return {
            "url": self.url,
            "name": self.name,
            "title": self.title,
            "country": self.country,
            "category": self.category,
            "description": self.description,
            "entry_point": self.entry_point,
            "data": self.data.to_dict(),
            "publisher": self.publisher.to_dict(),
        }

    @classmethod
    def _from_metadata(cls, file_path):
        with open(file_path, "r") as fh:
            config = yaml.load(fh, Loader=yaml.SafeLoader)
        return cls(file_path, config)

    @classmethod
    def _load_cache(cls):
        if not hasattr(cls, "_cache"):
            cls._cache = {}
            for glob in ("**/*.yml", "**/*.yaml"):
                for file_path in settings.METADATA_PATH.glob(glob):
                    dataset = cls._from_metadata(file_path)
                    cls._cache[dataset.name] = dataset
        return cls._cache

    @classmethod
    def all(cls):
        return cls._load_cache().values()

    @classmethod
    def get(cls, name):
        return cls._load_cache().get(name)

    @classmethod
    def names(cls):
        """An array of all dataset names found in the metadata path."""
        return [dataset.name for dataset in cls.all()]
