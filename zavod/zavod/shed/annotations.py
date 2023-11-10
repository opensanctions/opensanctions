import logging
from functools import cache
from typing import cast, Generator, Any, Dict, Optional, Set
from followthemoney.helpers import check_person_cutoff
from requests import Session
from nomenklatura.entity import CE
from nomenklatura.dataset import DS
from nomenklatura.cache import Cache
from nomenklatura.enrich.common import Enricher, EnricherConfig

from zavod.logs import get_logger
from zavod import settings


class AnnotationsEnricher(Enricher):

    def expand(self, entity: CE, match: CE) -> Generator[CE, None, None]:
        yield match

    def match(self, entity: CE) -> Generator[CE, None, None]:
        if not entity.schema.is_a("Position"):
            return
        if not entity.referents:
            return
        entity_ids = entity.referents
        entity_ids.add(entity.id)
        for entity_id in entity_ids:
            data = self.get_position(entity_id)
            if data is None:
                continue
            proxy = self.make_entity(entity, "Position")
            proxy.id = entity_id
            proxy.add("topics", data["topics"])
            if proxy.get("topics"):
                yield proxy

    def get_position(self, entity_id: str) -> Optional[Dict[str, Any]]:
        url = f"{settings.API_URL}/positions/{entity_id}"
        headers = {"authorization": settings.API_KEY}
        res = self.session.get(url, headers=headers)

        if res.status_code == 200:
            return res.json()
        elif res.status_code == 404:
            return None
        else:
            res.raise_for_status()