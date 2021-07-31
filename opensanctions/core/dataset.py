from banal import ensure_list
from urllib.parse import urljoin
from datapatch import get_lookups
from followthemoney import model
from followthemoney.types import registry

from opensanctions import settings
from opensanctions.core.entity import Entity
from opensanctions.helpers.lookups import load_yaml
from opensanctions.model import Issue, Statement, Resource
from opensanctions.model.base import KEY_LEN
from opensanctions.util import joinslug


class Dataset(object):
    """A dataset is a unit of execution of crawlers, and a grouping of entities.
    There are two types: sources (which relate to a specific crawlers), and
    collections (which group sources into more useful units)."""

    ALL = "all"

    def __init__(self, type_, file_path, config):
        self.type = type_
        self.file_path = file_path
        self.name = config.get("name", file_path.stem)
        self.prefix = config.get("prefix", self.name)
        self.title = config.get("title", self.name)
        self.summary = config.get("summary", "")
        self.description = config.get("description", "")

        # Collections can be part of other collections.
        collections = ensure_list(config.get("collections"))
        if self.name != self.ALL:
            collections.append(self.ALL)
        self.collections = set(collections)

        self.lookups = get_lookups(config.get("lookups", {}))

    def make_slug(self, *parts, strict=True):
        slug = joinslug(*parts, prefix=self.prefix, strict=strict)
        if slug is not None:
            return slug[:KEY_LEN]

    def make_entity(self, schema, target=False):
        return Entity(self, schema, target=target)

    def get_entity(self, entity_id):
        """Fetch an entity in the given dataset by its ID.

        If you run this in a crawler, you may want to run ``context.flush()``
        first to ensure all relevant entity fragments have been written to the
        database."""
        for entity in Entity.query(self, entity_id=entity_id):
            return entity

    @property
    def datasets(self):
        return set([self])

    @property
    def source_names(self):
        return [s.name for s in self.sources]

    @classmethod
    def _from_metadata(cls, file_path):
        from opensanctions.core.source import Source
        from opensanctions.core.collection import Collection

        config = load_yaml(file_path)
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
        return list(sorted((dataset.name for dataset in cls.all())))

    def to_dict(self):
        return {
            "name": self.name,
            "type": self.type,
            "title": self.title,
            "summary": self.summary,
            "description": self.description,
        }

    def make_public_url(self, path):
        """Generate a public URL for a file within the dataset context."""
        url = urljoin(settings.DATASET_URL, f"{self.name}/")
        return urljoin(url, path)

    def get_target_countries(self):
        countries = []
        for code, count in Statement.agg_target_by_country(dataset=self):
            result = {
                "code": code,
                "count": count,
                "label": registry.country.caption(code),
            }
            countries.append(result)
        return countries

    def get_target_schemata(self):
        schemata = []
        for name, count in Statement.agg_target_by_schema(dataset=self):
            schema = model.get(name)
            result = {
                "name": name,
                "count": count,
                "label": schema.label,
                "plural": schema.plural,
            }
            schemata.append(result)
        return schemata

    def to_index(self):
        meta = self.to_dict()
        meta["index_url"] = self.make_public_url("index.json")
        meta["issues_url"] = self.make_public_url("issues.json")
        meta["issue_levels"] = Issue.agg_by_level(dataset=self)
        meta["issue_count"] = sum(meta["issue_levels"].values())
        meta["target_count"] = Statement.all_counts(dataset=self, target=True)

        meta["targets"] = {
            "countries": self.get_target_countries(),
            "schemata": self.get_target_schemata(),
        }
        meta["resources"] = []
        for resource in Resource.query(dataset=self):
            res = resource.to_dict()
            res["url"] = self.make_public_url(resource.path)
            meta["resources"].append(res)
        return meta

    def __eq__(self, other):
        return self.name == other.name

    def __hash__(self):
        return hash(self.type + self.name)
