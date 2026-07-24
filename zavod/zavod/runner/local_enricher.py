import logging
from decimal import Decimal
from collections.abc import Generator, Iterator
from followthemoney import registry, model
from followthemoney.helpers import check_person_cutoff

from nomenklatura.enrich.common import EnricherConfig
from nomenklatura.enrich.common import EnrichmentException
from nomenklatura.enrich.common import BaseEnricher
from nomenklatura.matching import get_algorithm, EntityResolveRegression
from nomenklatura.blocker.index import BlockingMatches, Index
from nomenklatura.resolver import Identifier
from nomenklatura import Judgement
from nomenklatura.cache import Cache

from zavod.archive import dataset_state_path
from zavod.context import Context
from zavod.integration.dedupe import get_dataset_linker
from zavod.entity import Entity
from zavod.meta import Dataset, get_multi_dataset, get_catalog
from zavod.runner.util import check_enrich_topics, should_promote
from zavod.store import get_store, View
from zavod.reset import reset_caches

log = logging.getLogger(__name__)


class LocalEnricher(BaseEnricher[Dataset]):
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

    def __init__(self, dataset: Dataset, cache: Cache, config: EnricherConfig):
        super().__init__(dataset, cache, config)
        assert dataset.model.full_dataset is not None, (
            "LocalEnricher requires a target dataset name as `full_dataset`"
        )
        target_dataset = get_catalog().require(dataset.model.full_dataset)
        target_linker = get_dataset_linker(target_dataset)
        self.target_store = get_store(target_dataset, target_linker)
        self.target_store.sync()
        self.target_view = self.target_store.view(target_dataset)
        index_path = dataset_state_path(target_dataset.name) / "enrich-index"
        self._index = Index(
            self.target_view, index_path, config.get("index_options", {})
        )
        self._index.build()

        algo_name = config.get("algorithm", EntityResolveRegression.NAME)
        _algorithm = get_algorithm(algo_name)
        if _algorithm is None:
            raise Exception(f"Unknown algorithm: {algo_name}")
        self._algorithm = _algorithm
        self._algorithm_config = _algorithm.default_config()
        self._cutoff = float(config.get("cutoff", 0.5))
        self._limit = int(config.get("limit", 10))
        self._max_bin = int(config.get("max_bin", 10))

    def close(self) -> None:
        self.target_store.close()
        self._index.close()

    def candidates(
        self, subjects: Iterator[Entity]
    ) -> Generator[tuple[Identifier, BlockingMatches], None, None]:
        entity_generator = (e for e in subjects if self._filter_entity(e))
        yield from self._index.match_entities(entity_generator)

    def match_candidates(
        self, entity: Entity, candidates: BlockingMatches
    ) -> Generator[Entity, None, None]:
        # Make sure an entity with the same ID is yielded. E.g. a QID or ID scheme
        # intentionally consistent between datasets.
        assert entity.id is not None
        same_id_match = self.target_view.get_entity(entity.id)
        if same_id_match is not None:
            yield same_id_match

        scores: list[tuple[float, Entity]] = []
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

            result = self._algorithm.compare(entity, match, self._algorithm_config)
            if result.score < self._cutoff:
                continue

            scores.append((result.score, match))

        scores.sort(key=lambda s: s[0], reverse=True)
        for algo_score, proxy in scores[: self._limit]:
            yield proxy

    def _traverse_nested(
        self, entity: Entity, path: list[str] = []
    ) -> Generator[Entity, None, None]:
        """Expand starting from a match, recursing to related non-edge entities"""
        assert entity.id is not None

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
    enricher: LocalEnricher,
    entity: Entity,
    match: Entity,
    subject_view: View,
    topic_gated: bool,
    enrich_topics: frozenset[str],
) -> None:
    assert match.id is not None
    assert entity.id is not None
    if not entity.schema.can_match(match.schema):
        return None
    judgement = context.resolver.get_judgement(match.id, entity.id)

    if judgement == Judgement.NO_JUDGEMENT:
        context.emit(match, external=True)

    # Store previously confirmed matches to the database and make
    # them visible:
    if judgement == Judgement.POSITIVE:
        context.log.info(f"Enrich [{entity}]: {match!r}")
        expanded = list(enricher.expand_wrapped(entity, match))
        expanded = [adj for adj in expanded if not check_person_cutoff(adj)]

        if topic_gated:
            topic_matches = check_enrich_topics(expanded, subject_view, enrich_topics)
            for adj in expanded:
                context.emit(adj, external=not should_promote(adj, topic_matches))
        else:
            for adj in expanded:
                context.emit(adj, external=False)


def enrich(context: Context) -> None:
    scope = get_multi_dataset(context.dataset.inputs)
    # The Context resolver is read-only here (save_match only reads judgements),
    # so its load commits as a no-op along with the cache via context.close().
    context.log.info(f"Enriching {scope.name} ({[d.name for d in scope.datasets]})")

    config = dict(context.dataset.config)
    topic_gated: bool = bool(config.get("topic_gated", False))
    enricher = LocalEnricher(context.dataset, context.cache, config)
    enrich_topics: frozenset[str] = frozenset(enricher._filter_topics)
    if topic_gated and not enrich_topics:
        context.log.warning(
            "topic_gated=True but no topics configured; all expanded entities will be external"
        )

    subject_store = get_store(scope, context.resolver)
    # Commit the resolver's load-time read (and the cache-table DDL) so no
    # transaction is held open across the store sync and index build below; the
    # resolver is in-memory after the load.
    context.flush()
    subject_store.sync()
    subject_view = subject_store.view(scope, external=topic_gated)

    reset_caches()

    try:
        context.log.info("Matching candidates...")
        schemata = list(model.matchable_schemata())
        if len(enricher._filter_schemata):
            schemata = [s for s in schemata if s.name in enricher._filter_schemata]
        entities = subject_view.entities(include_schemata=schemata)
        candidates = enricher.candidates(entities)
        for entity_idx, (entity_id, candidate_set) in enumerate(candidates):
            if entity_idx > 0 and entity_idx % 100 == 0:
                context.flush()
            if entity_idx > 0 and entity_idx % 10000 == 0:
                context.log.info(f"Enriched {entity_idx} entities...")
            subject_entity = subject_view.get_entity(entity_id.id)
            if subject_entity is None:
                context.log.error(f"Missing entity: {entity_id!r}")
                continue
            try:
                for match in enricher.match_candidates(subject_entity, candidate_set):
                    save_match(
                        context,
                        enricher,
                        subject_entity,
                        match,
                        subject_view,
                        topic_gated,
                        enrich_topics,
                    )
            except EnrichmentException as exc:
                context.log.error(f"Enrichment error {subject_entity!r}: {str(exc)}")
        context.log.info("Enrichment process complete.")
    finally:
        enricher.close()
        subject_store.close()
