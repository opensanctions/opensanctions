from followthemoney.helpers import check_person_cutoff
from nomenklatura.judgement import Judgement
from nomenklatura.resolver import Resolver
from nomenklatura.enrich import (
    Enricher,
    ItemEnricher,
    BulkEnricher,
    EnrichmentException,
    make_enricher,
)

from zavod.meta import Dataset, get_multi_dataset
from zavod.entity import Entity
from zavod.context import Context
from zavod.dedupe import get_resolver
from zavod.store import get_store, View


def save_match(
    context: Context,
    resolver: Resolver[Entity],
    enricher: Enricher[Dataset],
    entity: Entity,
    match: Entity,
    threshold: float,
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
            # self.log.info("Added", entity=adjacent)
            context.emit(adjacent)


def enrich(context: Context) -> None:
    resolver = get_resolver()
    scope = get_multi_dataset(context.dataset.inputs)
    context.log.info(
        "Enriching %s (%s)" % (scope.name, [d.name for d in scope.datasets])
    )
    store = get_store(scope, resolver)
    store.sync()
    view = store.view(scope)
    config = dict(context.dataset.config)
    enricher = make_enricher(context.dataset, context.cache, config)
    if enricher is None:
        raise RuntimeError("Cannot load enricher: %r" % config)
    threshold = float(context.dataset.config.get("threshold", 0.7))
    try:
        if isinstance(enricher, BulkEnricher):
            enrich_bulk(context, resolver, enricher, view, threshold)
        elif isinstance(enricher, ItemEnricher):
            enrich_itemwise(context, resolver, enricher, view, threshold)
        else:
            raise RuntimeError("Unknown enricher type: %r" % enricher)

        resolver.save()
        context.log.info("Enrichment process complete.")
    finally:
        enricher.close()


def enrich_itemwise(
    context: Context,
    resolver: Resolver[Entity],
    enricher: BulkEnricher[Dataset],
    view: View[Dataset, Entity],
    threshold: float,
) -> None:
    for entity_idx, entity in enumerate(view.entities()):
        if entity_idx > 0 and entity_idx % 1000 == 0:
            context.cache.flush()
        if entity_idx > 0 and entity_idx % 10000 == 0:
            context.log.info("Enriched %s entities..." % entity_idx)
        context.log.debug("Enrich query: %r" % entity)
        try:
            for match in enricher.match_wrapped(entity):
                save_match(context, resolver, enricher, entity, match, threshold)
        except EnrichmentException as exc:
            context.log.error("Enrichment error %r: %s" % (entity, str(exc)))


def enrich_bulk(
    context: Context,
    resolver: Resolver[Entity],
    enricher: BulkEnricher[Dataset],
    view: View[Dataset, Entity],
    threshold: float,
) -> None:
    context.log.info("Loading entities for matching...")
    for entity_idx, entity in enumerate(view.entities()):
        try:
            enricher.load_wrapped(entity)
        except EnrichmentException as exc:
            context.log.error("Enrichment error %r: %s" % (entity, str(exc)))

    context.log.info("Matching candidates...")
    for entity_idx, (entity_id, candidate_set) in enumerate(enricher.candidates()):
        if entity_idx > 0 and entity_idx % 1000 == 0:
            context.cache.flush()
        if entity_idx > 0 and entity_idx % 10000 == 0:
            context.log.info("Enriched %s entities..." % entity_idx)
        entity = view.get_entity(entity_id.id)
        try:
            for match in enricher.match_candidates(entity, candidate_set):
                save_match(context, resolver, enricher, entity, match, threshold)
        except EnrichmentException as exc:
            context.log.error("Enrichment error %r: %s" % (entity, str(exc)))
