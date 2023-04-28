from functools import cache
from pathlib import Path
from typing import Any, Dict, Optional, Set, Tuple
from itertools import combinations
from collections import defaultdict

from nomenklatura.judgement import Judgement
from nomenklatura.resolver import Resolver, Identifier, StrIdent

from opensanctions import settings
from opensanctions.core.db import engine_read, engine_tx
from opensanctions.core.statements import resolve_canonical, entities_datasets
from opensanctions.core.entity import Entity
from opensanctions.core.dataset import Dataset
from opensanctions.core.loader import Database

AUTO_USER = "opensanctions/xref"
Scored = Tuple[str, str, Optional[float]]


class UniqueResolver(Resolver[Entity]):
    """OpenSanctions semantics for the entity resolver graph."""

    def decide(
        self,
        left_id: StrIdent,
        right_id: StrIdent,
        judgement: Judgement,
        user: Optional[str] = None,
        score: Optional[float] = None,
    ) -> Identifier:
        target = super().decide(left_id, right_id, judgement, user=user, score=score)
        if judgement == Judgement.POSITIVE:
            with engine_tx() as conn:
                resolve_canonical(conn, self, target.id)
        return target


@cache
def get_resolver() -> Resolver[Entity]:
    return UniqueResolver.load(Path(settings.RESOLVER_PATH))
