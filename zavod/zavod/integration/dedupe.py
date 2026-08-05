from typing import Any, TYPE_CHECKING

from pathlib import Path
from followthemoney import model
from nomenklatura.db import Session, make_session
from nomenklatura.xref import xref
from nomenklatura.resolver import Resolver, Linker
from nomenklatura.judgement import Judgement
from nomenklatura.matching import DefaultAlgorithm, get_algorithm

from zavod.entity import Entity
from zavod.logs import get_logger
from zavod.meta import Dataset
from zavod.integration.logic import logic_decide

if TYPE_CHECKING:
    from zavod.store import Store

log = get_logger(__name__)


def get_resolver(session: Session) -> Resolver[Entity]:
    """Load the deduplication resolver on the given session.

    The session owns the unit of work: callers commit/checkpoint it (e.g. a
    zavod Context shares its own session; a standalone CLI command owns one)."""
    resolver = Resolver[Entity](session, create=True)
    log.info(f"Using resolver: {resolver!r}")
    return resolver


def get_dataset_linker(dataset: Dataset) -> Linker[Entity]:
    """Get a resolver linker for the given dataset."""
    if not dataset.model.resolve:
        return Linker[Entity]({})
    with make_session() as session:
        resolver = get_resolver(session)
        log.info(f"Loading linker from: {resolver!r}")
        return resolver.get_linker()


def blocking_xref(
    resolver: Resolver[Entity],
    session: Session,
    store: "Store",
    state_path: Path,
    limit: int = 5000,
    patience: int = 500000,
    auto_threshold: float | None = None,
    algorithm: str = DefaultAlgorithm.NAME,
    focus_datasets: set[str] = set(),
    schema_range: str | None = None,
    user: str = "zavod/xref",
    discount_internal: float = 1.0,
    min_threshold: float = 0.01,
    blocker_options: dict[str, Any] | None = None,
) -> None:
    """This runs the deduplication process, which compares all entities in the given
    dataset against each other, and stores the highest-scoring candidates for human
    review. Candidates above the given threshold score will be merged automatically.
    """
    # resolver.prune()
    log.info(
        f"Xref running, algorithm: {algorithm!r}",
        auto_threshold=auto_threshold,
    )
    algorithm_type = get_algorithm(algorithm)
    if algorithm_type is None:
        raise ValueError(f"Invalid algorithm: {algorithm}")
    range = model.get(schema_range) if schema_range is not None else None
    index_dir = state_path / "dedupe-index"

    xref(
        resolver,
        session,
        store,
        index_dir=index_dir,
        limit=limit,
        patience=patience,
        range=range,
        scored=True,
        auto_threshold=auto_threshold,
        focus_datasets=focus_datasets,
        algorithm=algorithm_type,
        min_threshold=min_threshold,
        discount_internal=discount_internal,
        heuristic=logic_decide,
        blocker_options=blocker_options,
        user=user,
    )


def explode_cluster(resolver: Resolver[Entity], entity_id: str) -> None:
    """Destroy a cluster of deduplication matches."""
    canonical_id = resolver.get_canonical(entity_id)
    for part_id in resolver.explode(canonical_id):
        log.info("Restore separate entity", entity=part_id)


def merge_entities(
    resolver: Resolver[Entity], entity_ids: list[str], force: bool = False
) -> str:
    """Merge multiple entities into a canonical identity. This should be really easy
    but there are cases where a negative (or "unsure") judgement has been made, and
    needs to be overridden. This is activated via the `force` flag."""
    if len(entity_ids) < 2:
        raise ValueError("Need multiple IDs to merge!")
    canonical_id = resolver.get_canonical(entity_ids[0])
    for other_id in entity_ids[1:]:
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
                        f"Cannot merge {canonical_id!r} and {other_id!r}: {edge!r} exists!"
                    )
        log.info(f"Merge: {other_id} -> {canonical_id}")
        canonical_id_ = resolver.decide(canonical_id, other_id, Judgement.POSITIVE)
        canonical_id = str(canonical_id_)
    log.info(f"Canonical: {canonical_id}")
    return str(canonical_id)
