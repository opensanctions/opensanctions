from typing import cast
from followthemoney.helpers import check_person_cutoff
from nomenklatura.judgement import Judgement
from nomenklatura.enrich import Enricher, EnrichmentAbort, EnrichmentException
from nomenklatura.matching import DefaultAlgorithm

from opensanctions.core.entity import Entity
from opensanctions.core.db import engine_tx
from opensanctions.core.context import Context
from opensanctions.core.dataset import Dataset
from opensanctions.core.external import External
from opensanctions.core.store import AppStore
from opensanctions.core.statements import cleanup_dataset


def save_match(
    context: Context,
    enricher: Enricher,
    entity: Entity,
    match: Entity,
    threshold: float,
):
    if not entity.schema.can_match(match.schema):
        return
    judgement = context.resolver.get_judgement(match.id, entity.id)

    # For unjudged candidates, compute a score and put it in the
    # xref cache so the user can decide:
    if judgement == Judgement.NO_JUDGEMENT:
        result = DefaultAlgorithm.compare(entity, match)
        score = result["score"]
        if threshold is None or score >= threshold:
            context.log.info("Match [%s]: %.2f -> %s" % (entity, score, match))
            context.resolver.suggest(entity.id, match.id, score, user="os-enrich")

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


def enrich(scope_name: str, external_name: str, threshold: float):
    scope = Dataset.require(scope_name)
    context = Context(Dataset.require(external_name))
    external = cast(External, context.dataset)
    context.bind()
    context.clear(data=False)
    store = AppStore(scope, context.resolver)
    store.build(external=False)
    entities = store.view(scope)
    enricher = external.get_enricher(context.cache)
    try:
        for entity_idx, entity in enumerate(entities):
            if entity_idx > 0 and entity_idx % 1000 == 0:
                context.cache.flush()
            context.log.debug("Enrich query: %r" % entity)
            try:
                for match in enricher.match_wrapped(entity):
                    save_match(context, enricher, entity, match, threshold)
            except EnrichmentException as exc:
                context.log.error("Enrichment error %r: %s" % (entity, str(exc)))
            # except Exception:
            #     context.log.exception("Could not match: %r" % entity)

        with engine_tx() as conn:
            cleanup_dataset(conn, context.dataset)
        context.resolver.save()
        return True
    except KeyboardInterrupt:
        return False
    except EnrichmentAbort:
        context.log.exception("Enrichment aborted!")
        return False
    except Exception as exc:
        context.log.exception("Enrichment failed: %s" % repr(exc))
        return False
    finally:
        enricher.close()
        context.close()
