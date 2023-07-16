from typing import Optional
from zavod.logs import get_logger
from nomenklatura.xref import xref
from nomenklatura.matching import DefaultAlgorithm, get_algorithm

from zavod.meta import Dataset
from zavod.dedupe import get_resolver, AUTO_USER
from opensanctions.core.store import get_store

log = get_logger(__name__)


def blocking_xref(
    dataset: Dataset,
    limit: int = 5000,
    auto_threshold: float = 0.990,
    algorithm: str = DefaultAlgorithm.NAME,
    focus_dataset: Optional[str] = None,
):
    resolver = get_resolver()
    resolver.prune()
    log.info(
        "Xref running, auto merge threshold: %f; algorithm: %r"
        % (auto_threshold, algorithm)
    )
    store = get_store(dataset, external=True)
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
