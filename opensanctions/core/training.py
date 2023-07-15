from typing import Dict, Optional, Set, Tuple, Generator
from itertools import combinations
from collections import defaultdict
from zavod.logs import get_logger
from nomenklatura.judgement import Judgement
from nomenklatura.resolver import Identifier

from opensanctions.core.dataset import Dataset
from opensanctions.core.entity import Entity
from opensanctions.core.catalog import get_catalog
from opensanctions.core.db import engine_read
from opensanctions.core.statements import entities_datasets
from opensanctions.core.store import Store, get_store
from opensanctions.core.resolver import get_resolver, Resolver

log = get_logger(__name__)


def get_parts(
    resolver: Resolver, datasets: Dict[str, Set[Dataset]], id: str
) -> Generator[Tuple[str, Dataset], None, None]:
    canonical_id = resolver.get_canonical(id)
    for ref in resolver.get_referents(canonical_id):
        if ref.startswith(Identifier.PREFIX):
            continue
        for ds in datasets.get(ref, []):
            yield ref, ds


def get_partial(
    resolver: Resolver, store: Store, spec: Tuple[str, Dataset]
) -> Optional[Entity]:
    id, ds = spec
    loader = store.view(ds, external=True)
    canonical = resolver.get_canonical(id)
    entity = loader.get_entity(canonical)
    if entity is None:
        return None
    entity.id = id
    return entity


def export_training_pairs(scope: Dataset):
    resolver = get_resolver()
    catalog = get_catalog()
    datasets: Dict[str, Set[Dataset]] = defaultdict(set)
    with engine_read() as conn:
        for entity_id, ds in entities_datasets(conn, scope):
            if ds not in scope.leaf_names:
                continue
            dsa = catalog.get(ds)
            if dsa is not None:
                datasets[entity_id].add(dsa)

    log.info("Loaded %d entity ID mappings..." % len(datasets))
    pairs: Dict[Tuple[Tuple[str, Dataset], Tuple[str, Dataset]], Judgement] = {}
    judgements: Dict[Judgement, int] = defaultdict(int)
    for canonical_id in resolver.canonicals():
        parts = list(get_parts(resolver, datasets, canonical_id))
        for left, right in combinations(parts, 2):
            left, right = max(left, right), min(left, right)
            pairs[(left, right)] = Judgement.POSITIVE
            judgements[Judgement.POSITIVE] += 1
        for edge in resolver.nodes[canonical_id]:
            if edge.judgement in (Judgement.NEGATIVE, Judgement.UNSURE):
                source_canonical = resolver.get_canonical(edge.source)
                other = edge.target if source_canonical == canonical_id else edge.source
                for other_part in get_parts(resolver, datasets, other):
                    for part in parts:
                        part, other_part = max(part, other_part), min(part, other_part)
                        # pairs[(part, other_part)] = edge.judgement
                        # Export unsure as negative:
                        pairs[(part, other_part)] = edge.judgement
                        judgements[edge.judgement] += 1

    log.info(
        "Computed %d potential pairs..." % len(pairs),
        positive=judgements.get(Judgement.POSITIVE, 0),
        negative=judgements.get(Judgement.NEGATIVE, 0),
        unsure=judgements.get(Judgement.UNSURE, 0),
    )
    store = get_store(scope, external=True)
    for idx, ((left, right), judgement) in enumerate(pairs.items()):
        if idx > 0 and idx % 10000 == 0:
            log.info("Exported %d pairs..." % idx)
        left_entity = get_partial(resolver, store, left)
        right_entity = get_partial(resolver, store, right)
        if left_entity is None or right_entity is None:
            continue

        yield {
            "left": left_entity.to_dict(),
            "right": right_entity.to_dict(),
            "judgement": judgement,
        }
