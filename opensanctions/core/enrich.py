from typing import cast
from followthemoney.helpers import check_person_cutoff
from nomenklatura.cache import ConnCache
from nomenklatura.judgement import Judgement
from nomenklatura.enrich import Enricher, EnrichmentAbort, EnrichmentException
from nomenklatura.matching import DefaultAlgorithm

from opensanctions.core.entity import Entity
from opensanctions.core.context import Context
from opensanctions.core.dataset import Dataset
from opensanctions.core.external import External
from opensanctions.core.loader import Database
from opensanctions.core.statements import lock_dataset, cleanup_dataset


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
    database = Database(scope, context.resolver, cached=False)
    loader = database.view(scope)
    conn_cache = ConnCache(context.cache, context.data_conn)
    enricher = external.get_enricher(conn_cache)
    # lock_dataset(context.data_conn, external)
    try:
        for entity_idx, entity in enumerate(loader):
            if entity_idx > 0 and entity_idx % 1000 == 0:
                context.commit()
                # lock_dataset(context.data_conn, external)
            context.log.debug("Enrich query: %r" % entity)
            try:
                for match in enricher.match_wrapped(entity):
                    save_match(context, enricher, entity, match, threshold)
            except EnrichmentException as exc:
                context.log.error("Enrichment error %r: %s" % (entity, str(exc)))
            # except Exception:
            #     context.log.exception("Could not match: %r" % entity)

        cleanup_dataset(context.data_conn, context.dataset)
        context.commit()
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
