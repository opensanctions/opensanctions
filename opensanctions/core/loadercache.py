from typing import Iterable, List, Tuple
from followthemoney import model
from followthemoney.types import registry
from followthemoney.property import Property
from nomenklatura import Loader, MemoryLoader, Resolver

from opensanctions.core.dataset import Dataset
from opensanctions.core.entity import Entity
from opensanctions.model import Statement


class CachedType(object):
    __slots__ = (
        "entity_id",
        "canonical_id",
        "schema",
        "target",
        "first_seen",
        "last_seen",
        "dataset",
    )

    def __init__(self, stmt: Statement):
        self.canonical_id = str(stmt.canonical_id)
        dataset = Dataset.get(stmt.dataset)
        if dataset is None:
            raise RuntimeError("Missing dataset: %r" % stmt.dataset)
        self.dataset = dataset
        self.schema = model.schemata[stmt.schema]
        self.entity_id = str(stmt.entity_id)
        self.first_seen = stmt.first_seen
        self.last_seen = stmt.last_seen
        self.target = stmt.target


class CachedProp(object):
    __slots__ = ("canonical_id", "value", "prop", "dataset")

    def __init__(self, stmt: Statement):
        self.canonical_id = str(stmt.canonical_id)
        dataset = Dataset.get(stmt.dataset)
        if dataset is None:
            raise RuntimeError("Missing dataset: %r" % stmt.dataset)
        self.dataset = dataset
        schema = model.schemata[stmt.schema]
        self.prop = schema.properties[stmt.prop]
        self.value = str(stmt.value)
        # if self.prop.type == registry.entity:
        #     self.value = resolver.get_canonical(stmt.value)


CachedEntity = Tuple[Tuple[CachedType, ...], Tuple[CachedProp, ...]]
