from followthemoney.helpers import check_person_cutoff
from nomenklatura.judgement import Judgement
from nomenklatura.resolver import Resolver
from nomenklatura.enrich import Enricher, EnrichmentException, get_enricher
from nomenklatura.matching import DefaultAlgorithm

from zavod.entity import Entity
from zavod.context import Context
from zavod.dedupe import get_resolver
from opensanctions.core.store import get_view  # type: ignore


def dataset_enricher(context: Context) -> Enricher:
    """Load and configure the enricher interface."""
    config = dict(context.dataset.config)
    enricher_type = config.pop("type")
    enricher_cls = get_enricher(enricher_type)
    if enricher_cls is None:
        raise RuntimeError("Could load enricher: %s" % enricher_type)
    return enricher_cls(context.dataset, context.cache, config)  # type: ignore


def save_match(
    context: Context,
    resolver: Resolver[Entity],
    enricher: Enricher,
    entity: Entity,
    match: Entity,
    threshold: float,
) -> None:
    if match.id is None or entity.id is None:
        return None
    if not entity.schema.can_match(match.schema):
        return None
    judgement = resolver.get_judgement(match.id, entity.id)

    # For unjudged candidates, compute a score and put it in the
    # xref cache so the user can decide:
    if judgement == Judgement.NO_JUDGEMENT:
        result = DefaultAlgorithm.compare(entity, match)
        score = result["score"]
        if threshold is None or score >= threshold:
            context.log.info("Match [%s]: %.2f -> %s" % (entity, score, match))
            resolver.suggest(entity.id, match.id, score, user="os-enrich")

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
    if context.dataset.scope is None:
        msg = "No enrichment scope defined for dataset: %s" % context.dataset.name
        raise RuntimeError(msg)
    view = get_view(context.dataset.scope)
    resolver = get_resolver()
    enricher = dataset_enricher(context)
    threshold = float(context.dataset.config.get("threshold", 0.7))
    try:
        for entity_idx, entity in enumerate(view.entities()):
            if entity_idx > 0 and entity_idx % 1000 == 0:
                context.cache.flush()
            context.log.debug("Enrich query: %r" % entity)
            try:
                for match in enricher.match_wrapped(entity):
                    save_match(context, resolver, enricher, entity, match, threshold)
            except EnrichmentException as exc:
                context.log.error("Enrichment error %r: %s" % (entity, str(exc)))
            # except Exception:
            #     context.log.exception("Could not match: %r" % entity)

        # with engine_tx() as conn:
        #     cleanup_dataset(conn, context.dataset)
        resolver.save()
        context.log.info("Enrichment process complete.")
    finally:
        enricher.close()
        context.close()
