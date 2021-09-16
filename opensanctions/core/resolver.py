from functools import lru_cache
from typing import Optional, Tuple
from followthemoney.dedupe.judgement import Judgement
from nomenklatura.resolver import Resolver, Identifier
from nomenklatura.xref import xref

from opensanctions import settings
from opensanctions.model import Statement
from opensanctions.core.dataset import Dataset
from opensanctions.core.loader import DatabaseLoader, DatasetMemoryLoader
from opensanctions.core.index import get_index

RESOLVER_PATH = settings.STATIC_PATH.joinpath("resolve.ijson")
Scored = Tuple[str, str, Optional[float]]


class UniqueResolver(Resolver):
    """OpenSanctions semantics for the entity resolver graph."""

    def check_candidate(self, left: Identifier, right: Identifier) -> bool:
        if not super().check_candidate(left, right):
            return False
        lefts = [c.id for c in self.connected(left)]
        rights = [c.id for c in self.connected(right)]
        if Statement.unique_conflict(lefts, rights):
            self.decide(left, right, Judgement.NEGATIVE)
            return False
        return True


@lru_cache(maxsize=None)
def get_resolver() -> Resolver:
    return UniqueResolver.load(RESOLVER_PATH)


def xref_datasets(base: Dataset, candidates: Dataset, limit: int = 15):
    resolver = get_resolver()
    entities = DatabaseLoader(candidates, resolver)
    loader = DatasetMemoryLoader(base, resolver)
    index = get_index(base, loader)
    xref(index, resolver, entities, limit=limit)
    resolver.save()
