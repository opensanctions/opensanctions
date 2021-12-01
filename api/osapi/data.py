from functools import cache
from typing import Generator, List, Optional, Set, Tuple
from followthemoney import model
from followthemoney.schema import Schema
from followthemoney.property import Property
from nomenklatura.loader import Loader
from opensanctions.model import Statement
from opensanctions.core.entity import Entity
from opensanctions.core.dataset import Dataset
from opensanctions.core.index import get_index as get_dataset_index
from opensanctions.core.resolver import get_resolver
from opensanctions.core.loader import Database

from osapi import settings
from osapi.models import FreebaseType
from osapi.models import FreebaseEntity, FreebaseProperty

resolver = get_resolver()


def get_scope() -> Dataset:
    scope = Dataset.get(settings.SCOPE_DATASET)
    if scope is None:
        raise RuntimeError("Cannot load dataset: %s" % settings.SCOPE_DATASET)
    return scope


@cache
def get_database(cached=False) -> Database:
    return Database(get_scope(), resolver, cached=cached)


@cache
def get_datasets() -> List[Dataset]:
    return get_scope().provided_datasets()


@cache
def get_schemata(dataset: Dataset) -> List[Schema]:
    schemata: List[Schema] = list()
    names = Statement.all_schemata(dataset=dataset)
    for name in names:
        schema = model.get(name)
        if schema is not None:
            schemata.append(schema)
    return schemata


def get_matchable_schemata(dataset: Dataset) -> Set[Schema]:
    schemata: Set[Schema] = set()
    for schema in get_schemata(dataset):
        if schema.matchable:
            schemata.update(schema.schemata)
    return schemata


def get_freebase_types(dataset: Dataset) -> List[FreebaseType]:
    return [get_freebase_type(s) for s in get_matchable_schemata(dataset)]


def get_freebase_type(schema: Schema) -> FreebaseType:
    return {
        "id": schema.name,
        "name": schema.plural,
        "description": schema.description or schema.label,
    }


def get_freebase_entity(proxy: Entity, score: float = 0.0) -> FreebaseEntity:
    return {
        "id": proxy.id,
        "name": proxy.caption,
        "type": [get_freebase_type(proxy.schema)],
        "score": score,
        "match": False,
    }


def get_freebase_property(prop: Property) -> FreebaseProperty:
    return {
        "id": prop.qname,
        "name": prop.label,
        "description": prop.description,
    }
