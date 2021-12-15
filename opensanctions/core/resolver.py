from functools import lru_cache
from typing import Dict, Optional, Set, Tuple
from itertools import combinations
from collections import defaultdict
from followthemoney.dedupe.judgement import Judgement
from nomenklatura.resolver import Resolver, Identifier, StrIdent

from opensanctions import settings
from opensanctions.model import Statement
from opensanctions.core.entity import Entity
from opensanctions.core.dataset import Dataset
from opensanctions.core.loader import Database

AUTO_USER = "opensanctions"
RESOLVER_PATH = settings.STATIC_PATH.joinpath("resolve.ijson")
Scored = Tuple[str, str, Optional[float]]


class UniqueResolver(Resolver[Entity]):
    """OpenSanctions semantics for the entity resolver graph."""

    def check_candidate(self, left: Identifier, right: Identifier) -> bool:
        if not super().check_candidate(left, right):
            return False
        if Identifier.QID.match(str(left)) and Identifier.QID.match(str(right)):
            return False
        lefts = [c.id for c in self.connected(left)]
        rights = [c.id for c in self.connected(right)]
        if Statement.unique_conflict(lefts, rights):
            self.decide(left, right, Judgement.NEGATIVE, user=AUTO_USER)
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


def export_pairs(dataset: Dataset):
    resolver = get_resolver()
    db = Database(dataset, resolver, cached=True)
    datasets: Dict[str, Set[Dataset]] = defaultdict(set)
    for entity_id, ds in Statement.entities_datasets(dataset):
        dsa = Dataset.get(ds)
        if dsa is not None:
            datasets[entity_id].add(dsa)

    def get_parts(id):
        canonical_id = resolver.get_canonical(id)
        for ref in resolver.get_referents(canonical_id):
            for ds in datasets.get(ref, []):
                yield ref, ds

    pairs: Dict[Tuple[Tuple[str, Dataset], Tuple[str, Dataset]], Judgement] = {}
    for canonical_id in resolver.canonicals():
        parts = list(get_parts(canonical_id))
        for left, right in combinations(parts, 2):
            left, right = max(left, right), min(left, right)
            pairs[(left, right)] = Judgement.POSITIVE
        for edge in resolver.nodes[canonical_id]:
            if edge.judgement == Judgement.NEGATIVE:
                source_canonical = resolver.get_canonical(edge.source)
                other = edge.target if source_canonical == canonical_id else edge.source
                for other_part in get_parts(other):
                    for part in parts:
                        part, other_part = max(part, other_part), min(part, other_part)
                        pairs[(part, other_part)] = Judgement.NEGATIVE

    def get_partial(spec):
        id, ds = spec
        loader = db.view(ds)
        canonical = resolver.get_canonical(id)
        entity = loader.get_entity(canonical)
        if entity is not None:
            return entity.to_nested_dict(loader)

    for (left, right), judgement in pairs.items():
        # yield [left[0], right[0], judgement]
        left_entity = get_partial(left)
        right_entity = get_partial(right)
        if left_entity is not None and right_entity is not None:
            yield {"left": left_entity, "right": right_entity, "judgement": judgement}
