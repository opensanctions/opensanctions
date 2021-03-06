import yaml
from banal import ensure_list
from ftmstore import get_dataset as get_store

from opensanctions import settings


class Target(object):
    """A target (think: Makefile target) is a unit of execution of crawlers, and
    a grouping of data. There are two types: datasets (which relate to a specific
    data source), and collections (which group datasets into more useful units)."""

    ALL = "all"

    def __init__(self, type_, file_path, config):
        self.type = type_
        self.file_path = file_path
        self.name = config.get("name", file_path.stem)
        self.title = config.get("title", self.name)
        self.description = config.get("description", "")

        # Collections can be part of other collections.
        collections = ensure_list(config.get("collections"))
        if self.name != self.ALL:
            collections.append(self.ALL)
        self.collections = set(collections)

    @property
    def store(self):
        name = f"{self.type}_{self.name}"
        return get_store(name, database_uri=settings.DATABASE_URI)

    @classmethod
    def _from_metadata(cls, file_path):
        from opensanctions.core.source import Source
        from opensanctions.core.collection import Collection

        with open(file_path, "r") as fh:
            config = yaml.load(fh, Loader=yaml.SafeLoader)

        type_ = config.get("type", Source.TYPE)
        type_ = type_.lower().strip()
        if type_ == Collection.TYPE:
            return Collection(file_path, config)
        if type_ == Source.TYPE:
            return Source(file_path, config)

    @classmethod
    def _load_cache(cls):
        if not hasattr(cls, "_cache"):
            cls._cache = {}
            for glob in ("**/*.yml", "**/*.yaml"):
                for file_path in settings.METADATA_PATH.glob(glob):
                    target = cls._from_metadata(file_path)
                    cls._cache[target.name] = target
        return cls._cache

    @classmethod
    def all(cls):
        return cls._load_cache().values()

    @classmethod
    def get(cls, name):
        return cls._load_cache().get(name)

    @classmethod
    def names(cls):
        """An array of all target names found in the metadata path."""
        return [target.name for target in cls.all()]

    def to_dict(self):
        return {
            "name": self.name,
            "type": self.type,
            "title": self.title,
            "description": self.description,
        }

    def __eq__(self, other):
        return self.name == other.name

    def __hash__(self):
        return hash(self.name)
