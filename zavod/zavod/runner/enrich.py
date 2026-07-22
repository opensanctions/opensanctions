from followthemoney import registry
from followthemoney.helpers import check_person_cutoff
from nomenklatura.judgement import Judgement
from nomenklatura.enrich import Enricher, EnrichmentException, make_enricher

from zavod.meta import Dataset, get_multi_dataset
from zavod.entity import Entity
from zavod.context import Context
from zavod.runner.util import check_publishability, should_promote
from zavod.store import get_store, View


def save_match(
    context: Context,
    enricher: Enricher[Dataset],
    entity: Entity,
    match: Entity,
    subject_view: View,
) -> None:
    if match.id is None or entity.id is None:
        return None
    if not entity.schema.can_match(match.schema):
        return None
    judgement = context.resolver.get_judgement(match.id, entity.id)

    if judgement == Judgement.NO_JUDGEMENT:
        context.emit(match, external=True)

    # Store previously confirmed matches to the database and make them visible:
    if judgement == Judgement.POSITIVE:
        context.log.info("Enrich [%s]: %r" % (entity, match))
        expanded = [adjacent for adjacent in enricher.expand_wrapped(entity, match)]
        if not expanded:
            return

        # The first expansion result is the confirmed match itself. Keep it
        # visible; gate the graph context that follows it on risk topics assigned
        # by the subject datasets and analyzers or being supporting schemata.
        context.emit(expanded[0])
        adjacent = [e for e in expanded[1:] if not check_person_cutoff(e)]
        publishable = check_publishability(
            adjacent, subject_view, frozenset(registry.topic.RISKS)
        )
        for adjacent_entity in adjacent:
            context.emit(
                adjacent_entity,
                external=not should_promote(adjacent_entity, publishable),
            )


def enrich(context: Context) -> None:
    scope = get_multi_dataset(context.dataset.inputs)
    context.log.info(
        "Enriching %s (%s)" % (scope.name, [d.name for d in scope.datasets])
    )
    store = get_store(scope, context.resolver)
    # Commit the resolver's load-time read so no transaction is held open across
    # the (potentially long) store sync below; the resolver is in-memory after.
    context.flush()
    store.sync()
    view = store.view(scope, external=True)
    enricher = make_enricher(
        context.dataset,
        context.cache,
        dict(context.dataset.config),
        http_session=context.http,
    )
    try:
        for entity_idx, entity in enumerate(view.entities()):
            if entity_idx > 0 and entity_idx % 100 == 0:
                context.flush()
            if entity_idx > 0 and entity_idx % 10000 == 0:
                context.log.info("Enriched %s entities..." % entity_idx)
            context.log.debug("Enrich query: %r" % entity)
            try:
                for match in enricher.match_wrapped(entity):
                    save_match(context, enricher, entity, match, view)
            except EnrichmentException as exc:
                context.log.error("Enrichment error %r: %s" % (entity, str(exc)))
        context.log.info("Enrichment process complete.")
    finally:
        enricher.close()
