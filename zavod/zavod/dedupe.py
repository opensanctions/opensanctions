from typing import List, Optional, TYPE_CHECKING
from pathlib import Path
from functools import cache
from zavod.entity import Entity
from followthemoney import model
from nomenklatura.xref import xref
from nomenklatura.resolver import Resolver, Identifier
from nomenklatura.judgement import Judgement
from nomenklatura.matching import DefaultAlgorithm, get_algorithm

from zavod import settings
from zavod.logs import get_logger
from zavod.meta import Dataset

if TYPE_CHECKING:
    from zavod.store import Store

log = get_logger(__name__)
AUTO_USER = "zavod/xref"


@cache
def get_resolver() -> Resolver[Entity]:
    """Load the deduplication resolver."""
    if settings.RESOLVER_PATH is None:
        raise RuntimeError("Please set $ZAVOD_RESOLVER_PATH.")
    log.info("Loading resolver from: %s" % settings.RESOLVER_PATH)
    return Resolver.load(Path(settings.RESOLVER_PATH))


def get_dataset_resolver(dataset: Dataset) -> Resolver[Entity]:
    """Get a resolver for the given dataset."""
    if not dataset.resolve:
        return Resolver()
    return get_resolver()


def blocking_xref(
    store: "Store",
    limit: int = 5000,
    auto_threshold: Optional[float] = None,
    algorithm: str = DefaultAlgorithm.NAME,
    focus_dataset: Optional[str] = None,
    schema_range: Optional[str] = None,
) -> None:
    """This runs the deduplication process, which compares all entities in the given
    dataset against each other, and stores the highest-scoring candidates for human
    review. Candidates above the given threshold score will be merged automatically.
    """
    resolver = get_resolver()
    resolver.prune()
    log.info("Xref running, algorithm: %r" % algorithm, auto_threshold=auto_threshold)
    algorithm_type = get_algorithm(algorithm)
    if algorithm_type is None:
        raise ValueError("Invalid algorithm: %s" % algorithm)
    range = model.get(schema_range) if schema_range is not None else None
    xref(
        store,
        limit=limit,
        range=range,
        scored=True,
        auto_threshold=auto_threshold,
        focus_dataset=focus_dataset,
        algorithm=algorithm_type,
        user=AUTO_USER,
    )
    resolver.save()


def explode_cluster(entity_id: str) -> None:
    """Destroy a cluster of deduplication matches."""
    resolver = get_resolver()
    canonical_id = resolver.get_canonical(entity_id)
    for part_id in resolver.explode(canonical_id):
        log.info("Restore separate entity", entity=part_id)
    resolver.save()


def merge_entities(entity_ids: List[str], force: bool = False) -> str:
    """Merge multiple entities into a canonical identity. This should be really easy
    but there are cases where a negative (or "unsure") judgement has been made, and
    needs to be overridden. This is activated via the `force` flag."""
    if len(entity_ids) < 2:
        raise ValueError("Need multiple IDs to merge!")
    resolver = get_resolver()
    canonical_id = resolver.get_canonical(entity_ids[0])
    for other_id_ in entity_ids[1:]:
        other_id = Identifier.get(other_id_)
        other_canonical_id = resolver.get_canonical(other_id)
        if other_canonical_id == canonical_id:
            continue
        check = resolver.check_candidate(canonical_id, other_id)
        if not check:
            edge = resolver.get_resolved_edge(canonical_id, other_id)
            if edge is not None:
                if force is True or edge.judgement == Judgement.UNSURE:
                    log.warn("Removing existing edge", edge=edge)
                    resolver._remove_edge(edge)
                else:
                    raise ValueError(
                        "Cannot merge %r and %r: %r exists!"
                        % (canonical_id, other_id, edge)
                    )
        log.info("Merge: %s -> %s" % (other_id, canonical_id))
        canonical_id_ = resolver.decide(canonical_id, other_id, Judgement.POSITIVE)
        canonical_id = str(canonical_id_)
    resolver.save()
    log.info("Canonical: %s" % canonical_id)
    return str(canonical_id)
