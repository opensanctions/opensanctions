from typing import List, Set
from asyncstdlib.functools import cache
from followthemoney import model
from followthemoney.schema import Schema
from followthemoney.property import Property
from opensanctions.core.db import with_conn
from opensanctions.core.statements import all_schemata
from opensanctions.core.entity import Entity
from opensanctions.core.dataset import Dataset
from opensanctions.core.resolver import get_resolver
from opensanctions.core.loader import Database

from osapi import settings
from osapi.models import FreebaseType
from osapi.models import FreebaseEntity, FreebaseProperty


def get_scope() -> Dataset:
    scope = Dataset.get(settings.SCOPE_DATASET)
    if scope is None:
        raise RuntimeError("Cannot load dataset: %s" % settings.SCOPE_DATASET)
    return scope


@cache
async def get_database(cached=False) -> Database:
    resolver = await get_resolver()
    return Database(get_scope(), resolver, cached=cached)


def get_datasets() -> List[Dataset]:
    return get_scope().provided_datasets()


@cache
async def get_schemata(dataset: Dataset) -> List[Schema]:
    schemata: List[Schema] = list()
    async with with_conn() as conn:
        names = await all_schemata(conn, dataset=dataset)
    for name in names:
        schema = model.get(name)
        if schema is not None:
            schemata.append(schema)
    return schemata


async def get_matchable_schemata(dataset: Dataset) -> Set[Schema]:
    schemata: Set[Schema] = set()
    direct_schemata = await get_schemata(dataset)
    for schema in direct_schemata:
        if schema.matchable:
            schemata.update(schema.schemata)
    return schemata


async def get_freebase_types(dataset: Dataset) -> List[FreebaseType]:
    return [get_freebase_type(s) for s in await get_matchable_schemata(dataset)]


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
