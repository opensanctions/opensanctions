from functools import cache
from typing import Generator, List, Optional, Set, Tuple
from followthemoney import model
from followthemoney.schema import Schema
from followthemoney.property import Property
from nomenklatura.index.index import Index
from nomenklatura.loader import Loader
from api.osapi.models import FreebaseEntity, FreebaseProperty
from opensanctions.model import Statement
from opensanctions.core.entity import Entity
from opensanctions.core.dataset import Dataset
from opensanctions.core.index import get_index as get_dataset_index
from opensanctions.core.resolver import get_resolver
from opensanctions.core.loader import DatabaseLoader

from osapi import settings
from osapi.models import FreebaseType


resolver = get_resolver()


def get_scope() -> Dataset:
    scope = Dataset.get(settings.SCOPE_DATASET)
    if scope is None:
        raise RuntimeError("Cannot load dataset: %s" % settings.SCOPE_DATASET)
    return scope


@cache
def get_datasets() -> List[Dataset]:
    datasets: List[Dataset] = []
    available = set(get_scope().source_names)
    for dataset in Dataset.all():
        required = set(dataset.source_names)
        matches = available.intersection(required)
        if len(matches) == len(required):
            datasets.append(dataset)
    return datasets


@cache
def get_schemata(dataset: Dataset) -> List[Schema]:
    schemata: List[Schema] = list()
    names = Statement.all_schemata(dataset=dataset)
    for name in names:
        schema = model.get(name)
        if schema is not None:
            schemata.append(schema)
    return schemata


def get_matchable_schemata(dataset: Dataset):
    return [s for s in get_schemata(dataset) if s.matchable]


@cache
def get_loader(dataset: Dataset) -> Loader[Dataset, Entity]:
    return DatabaseLoader(dataset, resolver)


@cache
def get_index() -> Index[Dataset, Entity]:
    scope = get_scope()
    loader = get_loader(scope)
    return get_dataset_index(scope, loader)


@cache
def get_entity(dataset: Dataset, entity_id: str) -> Optional[Entity]:
    loader = get_loader(dataset)
    return loader.get_entity(entity_id)


def match_entities(
    dataset: Dataset, query: Entity, limit: int = 5, fuzzy: bool = False
) -> Generator[Tuple[Entity, float], None, None]:
    index = get_index()
    returned = 0
    for entity_id, score in index.match(query, limit=None, fuzzy=fuzzy):
        if returned >= limit:
            break
        entity = get_entity(dataset, entity_id)
        if entity is None:
            continue
        yield entity, score
        returned += 1


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
        # "n:type": {"id": "/properties/property", "name": "Property"},
    }
