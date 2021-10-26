from functools import cache
from typing import List, Set
from followthemoney.schema import Schema
from followthemoney.property import Property
from nomenklatura.index.index import Index
from nomenklatura.loader import Loader
from opensanctions.model import Statement
from opensanctions.core.entity import Entity
from opensanctions.core.dataset import Dataset
from opensanctions.core.resolver import get_resolver
from opensanctions.core.loader import DatasetMemoryLoader
from functools import cache

from osapi import settings
from osapi.models import FreebaseType


dataset = Dataset.get(settings.DATASET)
if dataset is None:
    raise RuntimeError("Cannot load dataset: %s" % settings.DATASET)
schemata = Statement.all_schemata(dataset=dataset)
resolver = get_resolver()


@cache
def get_schemata() -> List[Schema]:
    unique: Set[Schema] = set()
    for schema in schemata:
        if schema is not None:
            unique.update(schema.schemata)
    return list(unique)


def get_matchable_schemata():
    return [s for s in get_schemata() if s.matchable]


@cache
def get_loader() -> Loader[Dataset, Entity]:
    if dataset is None:
        raise RuntimeError("Unkown dataset")
    return DatasetMemoryLoader(dataset, resolver)


@cache
def get_index(loader: Loader[Dataset, Entity]) -> Index[Dataset, Entity]:
    index = Index(loader)
    index.build()
    return index


def get_freebase_types():
    return [get_freebase_type(s) for s in get_matchable_schemata()]


def get_freebase_type(schema: Schema) -> FreebaseType:
    return {"id": schema.name, "name": schema.label}


def get_freebase_entity(proxy: Entity, score: float = 0.0):
    return {
        "id": proxy.id,
        "name": proxy.caption,
        "type": [get_freebase_type(proxy.schema)],
        "score": score,
        "match": False,
    }


def get_freebase_property(prop: Property):
    return {
        "id": prop.qname,
        "name": prop.label,
        "description": prop.description,
        "n:type": {"id": "/properties/property", "name": "Property"},
    }
