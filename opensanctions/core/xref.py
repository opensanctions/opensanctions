import structlog
from nomenklatura.xref import xref
from followthemoney.types import registry

from opensanctions.core.dataset import Dataset
from opensanctions.core.loader import Database
from opensanctions.core.resolver import get_resolver

log = structlog.get_logger(__name__)


def blocking_xref(dataset: Dataset, limit: int = 5000):
    resolver = get_resolver()
    resolver.prune()
    db = Database(dataset, resolver, cached=True)
    loader = db.view(dataset)
    xref(loader, resolver, limit=limit, scored=True)
    resolver.save()
