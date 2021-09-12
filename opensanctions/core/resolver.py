from functools import lru_cache
from typing import Generator, Optional, Tuple
from followthemoney.dedupe.judgement import Judgement
from nomenklatura.resolver import Resolver, Identifier
from nomenklatura.xref import xref

from opensanctions import settings
from opensanctions.model import Statement
from opensanctions.core.dataset import Dataset
from opensanctions.core.entity import Entity
from opensanctions.core.loader import DatasetMemoryLoader
from opensanctions.core.index import get_index

RESOLVER_PATH = settings.STATIC_PATH.joinpath("resolve.ijson")
Scored = Tuple[str, str, Optional[float]]


class UniqueResolver(Resolver):
    """OpenSanctions semantics for the entity resolver graph."""

    def get_candidates(self, limit: int = 15) -> Generator[Scored, None, None]:
        """Check if the candidates are contradicted by an in-datset unique statement
        on the source data entities."""
        returned = 0
        candidates = super().get_candidates(limit=len(self.edges))
        for (target, source, score) in candidates:
            targets = [c.id for c in self.connected(Identifier.get(target))]
            sources = [c.id for c in self.connected(Identifier.get(source))]
            if Statement.unique_conflict(targets, sources):
                self.decide(target, source, Judgement.NEGATIVE)
                continue
            yield (target, source, score)
            returned += 1
            if returned >= limit:
                break


@lru_cache(maxsize=None)
def get_resolver() -> Resolver:
    return UniqueResolver.load(RESOLVER_PATH)


def xref_datasets(base: Dataset, candidates: Dataset, limit: int = 15):
    resolver = get_resolver()
    entities = Entity.query(candidates)
    loader = DatasetMemoryLoader(base)
    index = get_index(base, loader)
    xref(index, resolver, entities, limit=limit)
    resolver.save()
