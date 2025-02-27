from decimal import Decimal
import logging
from typing import Generator, List, Tuple
from followthemoney.types import registry
from followthemoney.helpers import check_person_cutoff

from nomenklatura.enrich.common import EnricherConfig
from nomenklatura.enrich.common import EnrichmentException
from nomenklatura.enrich.common import BaseEnricher
from nomenklatura.matching import get_algorithm, LogicV1
from nomenklatura.resolver import Identifier
from nomenklatura.judgement import Judgement
from nomenklatura.resolver import Resolver
from nomenklatura.dataset import DS
from nomenklatura.cache import Cache

from zavod.archive import dataset_state_path
from zavod.context import Context
from zavod.integration.dedupe import get_dataset_linker, get_resolver
from zavod.entity import Entity
from zavod.meta import Dataset, get_multi_dataset, get_catalog
from zavod.store import get_store
from zavod.reset import reset_caches
from zavod.integration.duckdb_index import DuckDBIndex, BlockingMatches


log = logging.getLogger(__name__)


class LocalEnricher(BaseEnricher[DS]):
    """
    Uses a local index to look up entities in a given dataset.

    Candidates are selected for matching using search index. Candidates are then scored
    by the matching algorithm to determine if they are a match.

    Entities that have the same rounded score from the blocking index can be
    considered to be binned together. Many match candidates with very similar names
    might score the same, or similarly and only one or a small number might eventually be
    considered a match.

    You don't want to cut off scoring too early using `index_options.max_candidates`,
    so use `max_bin` to configure the number of bins to step through before halting
    a given search.

    e.g. if the second 116 in index scores 118, 118, 118, 116, 116, 107 is the
    positive match, cutting off at rank 4 would miss it out, but cutting off at bin 2
    means all 116s are considered, the positive match is included. Other cases where
    index scores are more spread out would score a smaller number of items.

    Args:
        `config`: a dictionary of configuration options.
          `dataset`: `str` - the name of the dataset to enrich against.
          `cutoff`: `float` - (default 0.5) the minimum score required to be a match.
          `limit`: `int` - (default 5) the maximum number of top scoring matches
            to return.
          `max_bin`: `int` - (default 10) the maximum number of rounded index score
            bins to consider from a given search result.
          `algorithm`: `str` (default logic-v1) - the name of the algorithm
              to use for matching.
          `index_options`: `dict` - options to pass to the index.

    """

    def __init__(self, dataset: DS, cache: Cache, config: EnricherConfig):
        super().__init__(dataset, cache, config)
        target_dataset_name = config["dataset"]
        target_dataset = get_catalog().require(target_dataset_name)
        target_linker = get_dataset_linker(target_dataset)
        self.target_store = get_store(target_dataset, target_linker)
        self.target_store.sync()
        self.target_view = self.target_store.view(target_dataset)
        index_path = dataset_state_path(target_dataset_name) / "enrich-index"
        self._index = DuckDBIndex(
            self.target_view, index_path, config.get("index_options", {})
        )
        self._index.build()

        algo_name = config.get("algorithm", LogicV1.NAME)
        _algorithm = get_algorithm(algo_name)
        if _algorithm is None:
            raise Exception(f"Unknown algorithm: {algo_name}")
        self._algorithm = _algorithm
        self._cutoff = float(config.get("cutoff", 0.5))
        self._limit = int(config.get("limit", 5))
        self._max_bin = int(config.get("max_bin", 10))

    def close(self) -> None:
        self.target_store.close()
        self._index.close()

    def load(self, entity: Entity) -> None:
        self._index.add_matching_subject(entity)

    def load_wrapped(self, entity: Entity) -> None:
        if not self._filter_entity(entity):
            return
        self.load(entity)

    def candidates(self) -> Generator[Tuple[Identifier, BlockingMatches], None, None]:
        yield from self._index.matches()

    def match_candidates(
        self, entity: Entity, candidates: BlockingMatches
    ) -> Generator[Entity, None, None]:
        # Make sure an entity with the same ID is yielded. E.g. a QID or ID scheme
        # intentionally consistent between datasets.
        if entity.id is not None:
            same_id_match = self.target_view.get_entity(entity.id)
            if same_id_match is not None:
                yield same_id_match

        scores: List[Tuple[float, Entity]] = []
        last_rounded_score = None
        bin = 0

        for match_id, index_score in candidates:
            rounded_score = round(Decimal(index_score), 0)
            if rounded_score != last_rounded_score:
                bin += 1
                last_rounded_score = rounded_score
            if bin >= self._max_bin:
                break

            match = self.target_view.get_entity(match_id.id)
            if match is None:
                continue

            if not entity.schema.can_match(match.schema):
                continue

            result = self._algorithm.compare(entity, match)
            if result.score < self._cutoff:
                continue

            scores.append((result.score, match))

        scores.sort(key=lambda s: s[0], reverse=True)
        for algo_score, proxy in scores[: self._limit]:
            yield proxy

    def _traverse_nested(
        self, entity: Entity, path: List[str] = []
    ) -> Generator[Entity, None, None]:
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
        for prop, adjacent in self.target_view.get_adjacent(entity):
            if prop.type != registry.entity:
                continue
            if adjacent.id in path:
                continue

            yield from self._traverse_nested(adjacent, next_path)

    def expand(self, match: Entity) -> Generator[Entity, None, None]:
        yield from self._traverse_nested(match)

    def expand_wrapped(
        self, entity: Entity, match: Entity
    ) -> Generator[Entity, None, None]:
        if not self._filter_entity(entity):
            return
        yield from self.expand(match)


def save_match(
    context: Context,
    resolver: Resolver[Entity],
    enricher: LocalEnricher[Dataset],
    entity: Entity,
    match: Entity,
) -> None:
    if match.id is None or entity.id is None:
        return None
    if not entity.schema.can_match(match.schema):
        return None
    judgement = resolver.get_judgement(match.id, entity.id)

    if judgement not in (Judgement.NEGATIVE, Judgement.POSITIVE):
        context.emit(match, external=True)

    # Store previously confirmed matches to the database and make
    # them visible:
    if judgement == Judgement.POSITIVE:
        context.log.info("Enrich [%s]: %r" % (entity, match))
        for adjacent in enricher.expand_wrapped(entity, match):
            if check_person_cutoff(adjacent):
                continue
            context.emit(adjacent)


def enrich(context: Context) -> None:
    resolver = get_resolver()
    scope = get_multi_dataset(context.dataset.inputs)
    context.log.info(
        "Enriching %s (%s)" % (scope.name, [d.name for d in scope.datasets])
    )
    subject_store = get_store(scope, resolver)
    subject_store.sync()
    subject_view = subject_store.view(scope)
    config = dict(context.dataset.config)
    enricher = LocalEnricher(context.dataset, context.cache, config)
    try:
        context.log.info("Loading entities for matching...")
        for entity in subject_view.entities():
            enricher.load_wrapped(entity)

        reset_caches()

        context.log.info("Matching candidates...")
        for entity_idx, (entity_id, candidate_set) in enumerate(enricher.candidates()):
            if entity_idx > 0 and entity_idx % 10000 == 0:
                context.log.info("Enriched %s entities..." % entity_idx)
            subject_entity = subject_view.get_entity(entity_id.id)
            if subject_entity is None:
                context.log.error("Missing entity: %r" % entity_id)
                continue
            try:
                for match in enricher.match_candidates(subject_entity, candidate_set):
                    save_match(context, resolver, enricher, subject_entity, match)
            except EnrichmentException as exc:
                context.log.error(
                    "Enrichment error %r: %s" % (subject_entity, str(exc))
                )
        resolver.save()
        context.log.info("Enrichment process complete.")
    finally:
        enricher.close()
        subject_store.close()
