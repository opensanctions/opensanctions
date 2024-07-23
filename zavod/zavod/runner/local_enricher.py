from collections import defaultdict
import logging
from typing import Generator, List, Tuple, Type
from followthemoney.types import registry

from nomenklatura import CompositeEntity
from nomenklatura.entity import CE
from nomenklatura.dataset import DS
from nomenklatura.cache import Cache
from nomenklatura.enrich.common import Enricher, EnricherConfig
from nomenklatura.enrich.common import EnrichmentException
from nomenklatura.index.tantivy_index import TantivyIndex
from nomenklatura.matching import get_algorithm, LogicV1
from nomenklatura.resolver.linker import Linker
from nomenklatura.judgement import Judgement

from zavod.entity import Entity
from zavod.archive import dataset_state_path
from zavod.meta import get_catalog
from zavod.store import get_store
from zavod.dedupe import get_resolver


log = logging.getLogger(__name__)


class LocalEnricher(Enricher):
    """
    Uses a local index to look up entities in a given dataset.

    Candidates are selected for matching using search index. Candidates are then scored
    by the matching algorithm to determine if they are a match.

    Args:
        `config`: a dictionary of configuration options.
          `dataset`: `str` - the name of the dataset to enrich against.
          `cutoff`: `float` - (default 0.5) the minimum score required to be a match.
          `limit`: `int` - (default 5) the maximum number of top scoring matches
            to return. It's possible for one more match to be returned if its ID matched.
          `algorithm`: `str` (default logic-v1) - the name of the algorithm
              to use for matching.
          `index_options`: `dict` - options to pass to the index.
            `max_candidates`: `int` - (default 100) the maximum number of search
              results to score.
            `memory_budget`: `int` - (default 100) the amount of memory to use for
              indexing in MB
            `threshold`: `float` - (default 1.0) the minimum tantivy score required
              to be scored by `algorithm`.
    """

    def __init__(self, dataset: DS, cache: Cache, config: EnricherConfig):
        super().__init__(dataset, cache, config)
        target_dataset_name = config.pop("dataset")
        target_dataset = get_catalog().require(target_dataset_name)
        # target_linker = get_dataset_linker(target_dataset)
        # TODO: Workaround until company register datasets have resolve key archived.
        # Should be latest 2024-07-20
        target_linker = Linker[Entity]({})
        target_store = get_store(target_dataset, target_linker)
        target_store.sync()
        self._view = target_store.view(target_dataset)
        index_path = dataset_state_path(dataset.name) / "tantivy-enrich-index"
        self._index = TantivyIndex(
            self._view, index_path, config.get("index_options", {})
        )
        self._index.build()

        algo_name = config.pop("algorithm", LogicV1.NAME)
        _algorithm = get_algorithm(algo_name)
        if _algorithm is None:
            raise EnrichmentException(f"Unknown algorithm: {algo_name}")
        self._algorithm = _algorithm
        self._cutoff = float(config.pop("cutoff", 0.5))
        self._limit = int(config.pop("limit", 5))
        self.resolver = get_resolver()
        self.ranks = defaultdict(set)


    def entity_from_statements(self, class_: Type[CE], entity: CompositeEntity) -> CE:
        if type(entity) is class_:
            return entity
        return class_.from_statements(self.dataset, entity.statements)

    def match(self, entity: CE) -> Generator[CE, None, None]:
        scores: List[Tuple[float, CE]] = []

        store_type_entity = self.entity_from_statements(
            self._view.store.entity_class, entity
        )

        # Make sure an entity with the same ID is yielded. E.g. a QID or ID scheme
        # intentionally consistent between datasets.
        if entity.id is not None:
            same_id_match = self._view.get_entity(entity.id)
            if same_id_match is not None:
                yield self.entity_from_statements(type(entity), same_id_match)

        for rank, (match_id, index_score) in enumerate(self._index.match(store_type_entity)):
            judgement = self.resolver.get_judgement(match_id, entity.id)
            if judgement == Judgement.POSITIVE:
                log.info("Rank %r score %.1f %s for %s", rank, index_score, match_id.id, entity.id)
                self.ranks[rank].add(match_id)

            match = self._view.get_entity(match_id.id)
            if match is None:
                continue

            if not entity.schema.can_match(match.schema):
                continue

            result = self._algorithm.compare(entity, match)
            if result.score < self._cutoff:
                continue

            proxy = self.entity_from_statements(type(entity), match)
            scores.append((result.score, proxy))

        scores.sort(key=lambda s: s[0], reverse=True)
        for algo_score, proxy in scores[: self._limit]:
            yield proxy

    def _traverse_nested(
        self, entity: CE, path: List[str] = []
    ) -> Generator[CE, None, None]:
        """Expand starting from a match, recursing to related non-edge entities"""
        if entity.id is None:
            return

        yield entity

        if len(path) > 1:
            return
        if not entity.schema.edge and len(path) > 0:
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
            yield from self._traverse_nested(proxy, next_path)

    def expand(self, entity: CE, match: CE) -> Generator[CE, None, None]:
        yield from self._traverse_nested(match)
    
    def log_ranks(self):
        for rank, matches in sorted(self.ranks.items()):
            log.info("Rank %r %r %r", rank, len(matches), [i.id for i in list(matches)[:3]])
