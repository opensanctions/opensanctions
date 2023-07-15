from functools import cache
from pathlib import Path
from zavod.entity import Entity
from typing import Optional, Tuple
from nomenklatura.judgement import Judgement
from nomenklatura.resolver import Resolver, Identifier, StrIdent

from opensanctions import settings
from opensanctions.core.db import engine_tx
from opensanctions.core.statements import resolve_canonical

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
