import logging
from typing import Generator, List, Optional, Type
from followthemoney.namespace import Namespace
from followthemoney.types import registry

from nomenklatura import CompositeEntity
from nomenklatura.entity import CE
from nomenklatura.dataset import DS
from nomenklatura.cache import Cache
from nomenklatura.enrich.common import Enricher, EnricherConfig
from nomenklatura.enrich.common import EnrichmentException
from nomenklatura.index.tantivy_index import TantivyIndex
from nomenklatura.matching import get_algorithm

from zavod.archive import dataset_state_path
from zavod.meta import get_catalog
from zavod.store import get_view


log = logging.getLogger(__name__)


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
            self._view, state_path, config.get("index_options", {})
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

    def entity_from_statements(self, class_: Type[CE], entity: CompositeEntity) -> CE:
        return class_.from_statements(self.dataset, entity.statements)

    def match(self, entity: CE) -> Generator[CE, None, None]:
        for match_id, index_score in self._index.match(entity):
            match = self._view.get_entity(match_id.id)
            if match is None:
                continue

            if not entity.schema.can_match(match.schema):
                continue

            if self._algorithm is None:
                raise EnrichmentException("No algorithm specified")
            result = self._algorithm.compare(entity, match)
            if result.score < self._threshold:
                continue

            proxy = self.entity_from_statements(type(entity), match)
            if self._ns is not None:
                proxy = self._ns.apply(proxy)

            yield proxy

    def _traverse_nested(
        self, entity: CE, path: List[str] = []
    ) -> Generator[CE, None, None]:
        if entity.id is None:
            return

        yield entity

        if len(path) > 1:
            return

        next_path = list(path)
        next_path.append(entity.id)
        store_type_entity = self.entity_from_statements(
            self._view.store.entity_class, entity
        )
        for prop, adjacent in self._view.get_adjacent(store_type_entity):
            if prop.type != registry.entity:
                continue
            if adjacent.id in path:
                continue

            proxy = self.entity_from_statements(type(entity), adjacent)
            if self._ns is not None:
                entity = self._ns.apply(proxy)
            yield from self._traverse_nested(proxy, next_path)

    def expand(self, entity: CE, match: CE) -> Generator[CE, None, None]:
        yield from self._traverse_nested(match)
