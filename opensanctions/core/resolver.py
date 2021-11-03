from functools import lru_cache
from typing import Optional, Tuple
from followthemoney.dedupe.judgement import Judgement
from nomenklatura.index.index import Index
from nomenklatura.resolver import Resolver, Identifier, StrIdent
from nomenklatura.xref import xref

from opensanctions import settings
from opensanctions.model import Statement
from opensanctions.core.entity import Entity
from opensanctions.core.dataset import Dataset
from opensanctions.core.loader import Database
from opensanctions.core.index import get_index

RESOLVER_PATH = settings.STATIC_PATH.joinpath("resolve.ijson")
Scored = Tuple[str, str, Optional[float]]


class UniqueResolver(Resolver[Entity]):
    """OpenSanctions semantics for the entity resolver graph."""

    def check_candidate(self, left: Identifier, right: Identifier) -> bool:
        if not super().check_candidate(left, right):
            return False
        lefts = [c.id for c in self.connected(left)]
        rights = [c.id for c in self.connected(right)]
        if Statement.unique_conflict(lefts, rights):
            self.decide(left, right, Judgement.NEGATIVE, user="opensanctions")
            return False
        return True

    def decide(
        self,
        left_id: StrIdent,
        right_id: StrIdent,
        judgement: Judgement,
        user: Optional[str] = None,
        score: Optional[float] = None,
    ) -> Identifier:
        target = super().decide(left_id, right_id, judgement, user=user, score=score)
        if judgement == Judgement.POSITIVE:
            Statement.resolve(self, target.id)
        return target


@lru_cache(maxsize=None)
def get_resolver() -> Resolver[Entity]:
    return UniqueResolver.load(RESOLVER_PATH)


def xref_datasets(base: Dataset, candidates: Dataset, limit: int = 15):
    resolver = get_resolver()
    resolver.prune()
    if candidates not in base.datasets:
        raise RuntimeError("%r is not contained in %r" % (candidates, base))
    db = Database(base, resolver, cached=True)
    entities = db.view(candidates)
    loader = db.view(base)
    index = get_index(base, loader)
    xref(index, resolver, entities, limit=limit)
    resolver.save()


def xref_internal(dataset: Dataset):
    resolver = get_resolver()
    resolver.prune()
    db = Database(dataset, resolver, cached=True)
    loader = db.view(dataset)
    index = Index(loader)
    index.build(fuzzy=False)
    for pair, score in index.pairs()[:5000]:
        left = loader.get_entity(str(pair[0]))
        right = loader.get_entity(str(pair[1]))
        if left is None or right is None:
            continue
        if left.schema not in right.schema.matchable_schemata:
            continue
        if not resolver.check_candidate(left.id, right.id):
            continue
        resolver.suggest(left.id, right.id, score)
    resolver.save()
