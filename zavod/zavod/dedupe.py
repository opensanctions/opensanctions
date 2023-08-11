from typing import Optional, TYPE_CHECKING
from pathlib import Path
from functools import cache
from zavod.entity import Entity
from nomenklatura.xref import xref
from nomenklatura.resolver import Resolver
from nomenklatura.matching import DefaultAlgorithm, get_algorithm

from zavod import settings
from zavod.logs import get_logger

if TYPE_CHECKING:
    from zavod.store import Store

log = get_logger(__name__)
AUTO_USER = "zavod/xref"


@cache
def get_resolver() -> Resolver[Entity]:
    """Load the deduplication resolver."""
    if settings.RESOLVER_PATH is None:
        raise RuntimeError("Please set $ZAVOD_RESOLVER_PATH.")
    log.info("Loading resolver from %s" % settings.RESOLVER_PATH)
    return Resolver.load(Path(settings.RESOLVER_PATH))


def blocking_xref(
    store: "Store",
    limit: int = 5000,
    auto_threshold: float = 0.990,
    algorithm: str = DefaultAlgorithm.NAME,
    focus_dataset: Optional[str] = None,
) -> None:
    """This runs the deduplication process, which compares all entities in the given
    dataset against each other, and stores the highest-scoring candidates for human
    review. Candidates above the given threshold score will be merged automatically.
    """
    resolver = get_resolver()
    resolver.prune()
    log.info(
        "Xref running, auto merge threshold: %f; algorithm: %r"
        % (auto_threshold, algorithm)
    )
    algorithm_type = get_algorithm(algorithm)
    if algorithm_type is None:
        raise ValueError("Invalid algorithm: %s" % algorithm)
    xref(
        store,
        limit=limit,
        scored=True,
        auto_threshold=auto_threshold,
        focus_dataset=focus_dataset,
        algorithm=algorithm_type,
        user=AUTO_USER,
    )
    resolver.save()
