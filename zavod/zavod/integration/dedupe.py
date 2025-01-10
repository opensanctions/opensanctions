from typing import List, Optional, TYPE_CHECKING
from pathlib import Path
from functools import cache
from zavod.entity import Entity
from followthemoney import model
from nomenklatura.xref import xref
from nomenklatura.resolver import Resolver, Identifier, Linker
from nomenklatura.judgement import Judgement
from nomenklatura.matching import DefaultAlgorithm, get_algorithm

from zavod import settings
from zavod.logs import get_logger
from zavod.meta import Dataset
from zavod.integration.duckdb_index import DuckDBIndex

if TYPE_CHECKING:
    from zavod.store import Store

log = get_logger(__name__)
AUTO_USER = "zavod/xref"


def _get_resolver_path() -> Path:
    """Get the path to the deduplication resolver."""
    if settings.RESOLVER_PATH is None:
        raise RuntimeError("Please set $ZAVOD_RESOLVER_PATH.")
    return Path(settings.RESOLVER_PATH)


def get_resolver() -> Resolver[Entity]:
    """Load the deduplication resolver."""
    path = _get_resolver_path()
    log.info("Loading resolver from: %s" % path.as_posix())
    # TODO maybe some env-var way to select between db and file
    return Resolver.load(path)


def get_dataset_linker(dataset: Dataset) -> Linker[Entity]:
    """Get a resolver linker for the given dataset."""
    if not dataset.resolve:
        return Linker[Entity]({})
    path = _get_resolver_path()
    log.info("Loading linker from: %s" % path.as_posix())
    return Resolver.load_linker(path)


def blocking_xref(
    resolver: Resolver[Entity],
    store: "Store",
    state_path: Path,
    limit: int = 5000,
    auto_threshold: Optional[float] = None,
    algorithm: str = DefaultAlgorithm.NAME,
    focus_dataset: Optional[str] = None,
    schema_range: Optional[str] = None,
    discount_internal: float = 1.0,
    conflicting_match_threshold: Optional[float] = None,
) -> None:
    """This runs the deduplication process, which compares all entities in the given
    dataset against each other, and stores the highest-scoring candidates for human
    review. Candidates above the given threshold score will be merged automatically.
    """
    resolver.prune()
    log.info(
        "Xref running, algorithm: %r" % algorithm,
        auto_threshold=auto_threshold,
    )
    algorithm_type = get_algorithm(algorithm)
    if algorithm_type is None:
        raise ValueError("Invalid algorithm: %s" % algorithm)
    range = model.get(schema_range) if schema_range is not None else None
    index_dir = state_path / "dedupe-index"

    xref(
        resolver,
        store,
        index_dir=index_dir,
        index_type=DuckDBIndex,
        limit=limit,
        range=range,
        scored=True,
        auto_threshold=auto_threshold,
        focus_dataset=focus_dataset,
        algorithm=algorithm_type,
        discount_internal=discount_internal,
        user=AUTO_USER,
        conflicting_match_threshold=conflicting_match_threshold,
    )


def explode_cluster(resolver: Resolver[Entity], entity_id: str) -> None:
    """Destroy a cluster of deduplication matches."""
    canonical_id = resolver.get_canonical(entity_id)
    for part_id in resolver.explode(canonical_id):
        log.info("Restore separate entity", entity=part_id)


def merge_entities(resolver: Resolver[Entity], entity_ids: List[str], force: bool = False) -> str:
    """Merge multiple entities into a canonical identity. This should be really easy
    but there are cases where a negative (or "unsure") judgement has been made, and
    needs to be overridden. This is activated via the `force` flag."""
    if len(entity_ids) < 2:
        raise ValueError("Need multiple IDs to merge!")
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
    log.info("Canonical: %s" % canonical_id)
    return str(canonical_id)
