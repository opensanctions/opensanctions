import os
from pathlib import Path
import time
import logging
from banal import ensure_list
from typing import Any, Generator, Optional, Dict, List
from urllib.parse import urljoin
from followthemoney.types import registry
from followthemoney.namespace import Namespace
from rigour.urls import build_url
# make temporary directory
from tempfile import TemporaryDirectory

from nomenklatura.entity import CE, CompositeEntity
from nomenklatura.dataset import DS
from nomenklatura.cache import Cache
from nomenklatura.enrich.common import Enricher, EnricherConfig
from nomenklatura.enrich.common import EnrichmentException
from nomenklatura import Index
from nomenklatura.matching import get_algorithm
from nomenklatura.resolver import Resolver
from nomenklatura.store.level import LevelDBStore
from zavod.meta import get_catalog
from zavod.store import get_store


log = logging.getLogger(__name__)

MATCH_CANDIDATES = 20


# need catalog to get dataset by name zavod.meta.get_catalog().require(dataset_)
# need a path to store the store zavod.store.get_store(dataset)


class LocalEnricher(Enricher):
    """Uses a local index to look up entities in a given dataset."""

    def __init__(self, dataset: DS, cache: Cache, config: EnricherConfig):
        super().__init__(dataset, cache, config)
        target_dataset_name = config.pop("dataset")
        target_dataset = get_catalog().require(target_dataset_name)
        store = get_store(target_dataset)
        self._view = store.default_view(external=False)
        self._index = Index(self._view)
        self._index.build()
        self._algorithm = get_algorithm(config.pop("algorithm", "best"))
        self._threshold = config.pop("threshold")
        
        
    def match(self, entity: CE) -> Generator[CE, None, None]:
        for match_id, index_score in self._index.match(entity)[:MATCH_CANDIDATES]:
            match = self._view.get_entity(match_id)
            
            if not entity.schema.can_match(match.schema):
                continue

            if index_score == 0:
                continue

            result = self._algorithm.compare(entity, match)
            if result.score >= self._threshold:
                yield match


    def expand(self, entity: CE, match: CE) -> Generator[CE, None, None]:
        # yield the entities that would be nested
        #raise NotImplementedError()
        yield match
