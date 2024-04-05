from typing import Dict, List, Tuple
from followthemoney.helpers import check_person_cutoff
from nomenklatura import Index
from nomenklatura.judgement import Judgement
from nomenklatura.resolver import Resolver, Pair
from nomenklatura.cache import Cache
from nomenklatura.enrich import Enricher, EnrichmentException, get_enricher
from nomenklatura.matching import DefaultAlgorithm

from zavod.meta import Dataset, get_multi_dataset
from zavod.entity import Entity
from zavod.context import Context
from zavod.dedupe import get_resolver
from zavod.store import get_view


def enrich(context: Context) -> None:
    # Get views
    context.log.info("Getting view of enrichment target dataset")
    target_view = get_view(context.dataset)
    context.log.info("Getting view of inputs")
    input_view = get_view(get_multi_dataset(context.dataset.inputs))

    # Dodgy index
    context.log.info("Building dodgy combined index")
    index = Index(target_view)
    for entity in target_view.entities():
        index.index(entity)
    for entity in input_view.entities():
        index.index(entity)
    index.commit()
    
    resolver = get_resolver()
    threshold = context.dataset.threshold
    
    # Get pairs - basically anything where something matches
    for idx, ((left_id, right_id), tf_score) in enumerate(index.pairs()):
        if not (
            (input_view.has_entity(left_id) and target_view.has_entity(right_id))
            or (input_view.has_entity(right_id) and target_view.has_entity(left_id))
        ):
            continue

        # decide which is the "match" and which is the subject
        if input_view.has_entity(left_id):
            entity = input_view.get_entity(left_id)
            match = target_view.get_entity(right_id)
        else:
            entity = input_view.get_entity(right_id)
            match = target_view.get_entity(left_id)

        if entity is None or entity.id is None or match is None or match.id is None:
            continue

        if not entity.schema.can_match(match.schema):
            continue

        judgement = resolver.get_judgement(left_id, right_id)

        # For unjudged candidates, compute a score and put it in the
        # xref cache so the user can decide:
        if judgement == Judgement.NO_JUDGEMENT:
            result = DefaultAlgorithm.compare(entity, match)
            if threshold is None or result.score >= threshold:
                context.log.info("Match [%s]: %.2f -> %s" % (entity, result.score, match))
                resolver.suggest(left_id, right_id, result.score, user="os-enrich")

        if judgement not in (Judgement.NEGATIVE, Judgement.POSITIVE):
            result = DefaultAlgorithm.compare(entity, match)
            if threshold is None or result.score >= threshold:
                context.emit(match, external=True)

        # Store previously confirmed matches to the database and make
        # them visible:
        if judgement == Judgement.POSITIVE:
            context.log.info("Enrich [%s]: %r" % (entity, match))
            #for adjacent in enricher.expand_wrapped(entity, match):
            #    if check_person_cutoff(adjacent):
            #        continue
            #    # self.log.info("Added", entity=adjacent)
            #    context.emit(adjacent)
