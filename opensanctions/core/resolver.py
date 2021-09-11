from functools import lru_cache
from nomenklatura.resolver import Resolver
from nomenklatura.xref import xref

from opensanctions import settings
from opensanctions.core.dataset import Dataset
from opensanctions.core.entity import Entity
from opensanctions.core.loader import DatasetMemoryLoader
from opensanctions.core.index import get_index

RESOLVER_PATH = settings.STATIC_PATH.joinpath("resolve.ijson")


@lru_cache(maxsize=None)
def get_resolver() -> Resolver:
    return Resolver.load(RESOLVER_PATH)


def xref_datasets(base: Dataset, candidates: Dataset):
    resolver = get_resolver()
    entities = Entity.query(candidates)
    loader = DatasetMemoryLoader(base)
    index = get_index(base, loader)
    xref(index, resolver, entities)
    resolver.save()
