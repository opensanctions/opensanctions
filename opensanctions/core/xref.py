from zavod.logs import get_logger
from nomenklatura.xref import xref

from opensanctions.core.dataset import Dataset
from opensanctions.core.loader import Database
from opensanctions.core.resolver import AUTO_USER, get_resolver

log = get_logger(__name__)


def blocking_xref(dataset: Dataset, limit: int = 5000, auto_threshold: float = 0.990):
    resolver = get_resolver()
    resolver.prune()
    log.info("Xref running, auto merge threshold: %f" % auto_threshold)
    db = Database(dataset, resolver, cached=False, external=True)
    loader = db.view(dataset)
    xref(
        loader,
        resolver,
        limit=limit,
        scored=True,
        auto_threshold=auto_threshold,
        user=AUTO_USER,
    )
    resolver.save()
