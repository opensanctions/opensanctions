from typing import Generator, Iterable

from followthemoney.helpers import check_person_cutoff
from nomenklatura.judgement import Judgement
from nomenklatura.resolver import Resolver
from nomenklatura.enrich import Enricher, EnrichmentException, make_enricher

from zavod.integration.dedupe import get_resolver
from zavod.meta import Dataset, get_multi_dataset
from zavod.entity import Entity
from zavod.context import Context
from zavod.store import get_store


def iter_entities(
    context: Context, entities: Iterable[Entity]
) -> Generator[Entity, None, None]:
    for entity_idx, entity in enumerate(entities):
        if entity_idx > 0 and entity_idx % 1000 == 0:
            context.flush()
        if entity_idx > 0 and entity_idx % 10000 == 0:
            context.log.info("Enriched %s entities..." % entity_idx)
        context.log.debug("Enrich query: %r" % entity)
        yield entity


def iter_matches(
    context: Context,
    enricher: Enricher[Dataset],
    entities: Iterable[Entity],
) -> Generator[tuple[Entity, Entity], None, None]:
    match_many_wrapped = getattr(enricher, "match_many_wrapped", None)
    if callable(match_many_wrapped):
        try:
            yield from match_many_wrapped(entities)
        except EnrichmentException as exc:
            context.log.error("Enrichment batch error: %s" % str(exc))
        return

    for entity in entities:
        try:
            for match in enricher.match_wrapped(entity):
                yield entity, match
        except EnrichmentException as exc:
            context.log.error("Enrichment error %r: %s" % (entity, str(exc)))


def save_match(
    context: Context,
    resolver: Resolver[Entity],
    enricher: Enricher[Dataset],
    entity: Entity,
    match: Entity,
) -> None:
    if match.id is None or entity.id is None:
        return None
    if not entity.schema.can_match(match.schema):
        return None
    judgement = resolver.get_judgement(match.id, entity.id)

    if judgement == Judgement.NO_JUDGEMENT:
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
    scope = get_multi_dataset(context.dataset.inputs)
    resolver = get_resolver()
    resolver.begin()
    context.log.info(
        "Enriching %s (%s)" % (scope.name, [d.name for d in scope.datasets])
    )
    store = get_store(scope, resolver)
    store.sync()
    view = store.view(scope)
    enricher = make_enricher(
        context.dataset,
        context.cache,
        dict(context.dataset.config),
        http_session=context.http,
    )
    try:
        entities = iter_entities(context, view.entities())
        for entity, match in iter_matches(context, enricher, entities):
            try:
                save_match(context, resolver, enricher, entity, match)
            except EnrichmentException as exc:
                context.log.error("Enrichment error %r: %s" % (entity, str(exc)))
        context.log.info("Enrichment process complete.")
    finally:
        resolver.rollback()
        enricher.close()
