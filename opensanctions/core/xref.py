from zavod.logs import get_logger
from nomenklatura.xref import xref

from opensanctions.core.dataset import Dataset
from opensanctions.core.loader import Database
from opensanctions.core.db import engine_tx
from opensanctions.core.statements import resolve_all_canonical
from opensanctions.core.resolver import AUTO_USER, get_resolver

log = get_logger(__name__)


def blocking_xref(dataset: Dataset, limit: int = 5000):
    resolver = get_resolver()
    with engine_tx() as conn:
        resolve_all_canonical(conn, resolver)
    resolver.prune()
    db = Database(dataset, resolver, cached=True, external=True)
    loader = db.view(dataset)
    xref(
        loader,
        resolver,
        limit=limit,
        scored=True,
        auto_threshold=0.990,
        user=AUTO_USER,
    )
    resolver.save()
