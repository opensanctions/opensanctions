import yaml
from ftmstore import get_dataset as get_store

from opensanctions import settings


class DatasetData(object):
    """Data source specification."""

    def __init__(self, config):
        self.url = config.get("url")
        self.format = config.get("format")

    def to_dict(self):
        return {"url": self.url, "fomat": self.format}


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
        self.data = DatasetData(config.get("data", {}))
        self.publisher = DatasetPublisher(config.get("publisher", {}))

    @property
    def store(self):
        return get_store(self.name, database_uri=settings.DATABASE_URI)

    def to_dict(self):
        return {
            "url": self.url,
            "name": self.name,
            "title": self.title,
            "country": self.country,
            "category": self.category,
            "description": self.description,
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
