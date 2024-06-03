import logging
from typing import Generator, List, Optional
from followthemoney.namespace import Namespace
from tempfile import mkdtemp
from followthemoney.types import registry

from nomenklatura.entity import CE
from nomenklatura.dataset import DS
from nomenklatura.cache import Cache
from nomenklatura.enrich.common import Enricher, EnricherConfig
from nomenklatura.enrich.common import EnrichmentException
from nomenklatura.index.tantivy_index import TantivyIndex
from nomenklatura.matching import get_algorithm

from zavod.archive import dataset_state_path
from zavod.meta import get_catalog
from zavod.store import get_store, get_view


log = logging.getLogger(__name__)

MATCH_CANDIDATES = 10


class LocalEnricher(Enricher):
    """
    Uses a local index to look up entities in a given dataset.

    Candidates are selected for matching based on the number of tokens
    they share with the entity being matched. Candidates are then scored
    by the matching algorithm to determine if they are a match.

    Configuration:
        `config.dataset`: `str` - the name of the dataset to enrich against.
        `config.threshold`: `float` - the threshold to be considered a match
            according to the matching algorithm used.
        `config.algorithm`: `str` (default logic-v1) - the name of the algorithm
            to use for matching.
    """

    def __init__(self, dataset: DS, cache: Cache, config: EnricherConfig):
        super().__init__(dataset, cache, config)
        target_dataset_name = config.pop("dataset")
        target_dataset = get_catalog().require(target_dataset_name)
        self._view = get_view(target_dataset, external=False)
        state_path = dataset_state_path(dataset.name)
        self._index = TantivyIndex(
            self._view, state_path, dataset.config.get("index_options", {})
        )
        self._index.build()
        algo_name = config.pop("algorithm", "logic-v1")
        self._algorithm = get_algorithm(algo_name)
        if self._algorithm is None:
            raise EnrichmentException(f"Unknown algorithm: {algo_name}")
        self._threshold = config.pop("threshold")
        self._ns: Optional[Namespace] = None
        if self.get_config_bool("strip_namespace"):
            self._ns = Namespace()

    def match(self, entity: CE) -> Generator[CE, None, None]:
        for match_id, index_score in self._index.match(entity)[:MATCH_CANDIDATES]:
            match = self._view.get_entity(match_id.id)
            if match is None:
                continue

            if not entity.schema.can_match(match.schema):
                continue

            if self._algorithm is None:
                raise EnrichmentException("No algorithm specified")
            result = self._algorithm.compare(entity, match)
            if result.score >= self._threshold:
                yield match

    def _traverse_nested(
        self, entity: CE, path: List[str] = []
    ) -> Generator[CE, None, None]:
        if self._ns is not None:
            entity = self._ns.apply(entity)

        yield entity

        if len(path) > 1:
            return

        next_path = list(path)
        next_path.append(entity.id)
        for prop, adjacent in self._view.get_adjacent(entity):
            if prop.type != registry.entity:
                continue
            if adjacent.id in path:
                continue
            yield from self._traverse_nested(adjacent, next_path)

    def expand(self, entity: CE, match: CE) -> Generator[CE, None, None]:
        yield from self._traverse_nested(match)
