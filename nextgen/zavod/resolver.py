from pathlib import Path
from functools import cache
from zavod.entity import Entity
from nomenklatura.resolver import Resolver

from zavod import settings

AUTO_USER = "zavod/xref"


@cache
def get_resolver() -> Resolver[Entity]:
    return Resolver.load(Path(settings.RESOLVER_PATH))
