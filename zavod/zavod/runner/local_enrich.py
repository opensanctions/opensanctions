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

    # Index
    index = Index(target_view)
    context.log.info("Indexing target entities")
    for entity in target_view.entities():
        index.index(entity)
    context.log.info("Indexing input entities")
    for entity in input_view.entities():
        index.index(entity)
    context.log.info("Committing index (counting)")
    index.commit()

    # Get pairs
    for idx, ((left_id, right_id), score) in enumerate(index.pairs()):
        # if idx % 1000 == 0 and idx > 0:
        #    context.log.info("Processed %d pairs" % idx)
        if not (
            (input_view.has_entity(left_id) and target_view.has_entity(right_id))
            or (input_view.has_entity(right_id) and target_view.has_entity(left_id))
        ):
            continue
        print(left_id, right_id, score)
