import structlog
from typing import Dict, Set
from itertools import combinations
from followthemoney.dedupe.judgement import Judgement
from nomenklatura import Resolver

from opensanctions.core.dataset import Dataset
from opensanctions.core.context import Context
from opensanctions.core.loader import Database
from opensanctions.core.entity import Entity

log = structlog.getLogger(__name__)
NOMINATIM = "https://nominatim.openstreetmap.org/search.php"
EXPIRE_CACHE = 84600 * 200


def query_nominatim(context: Context, address: Entity):
    for full in address.get("full"):
        params = {
            "q": full,
            "countrycodes": address.get("country"),
            "format": "jsonv2",
            "accept-language": "en",
            "addressdetails": 1,
        }
        results = context.fetch_json(NOMINATIM, params=params, cache_days=180)
        log.info(
            "OpenStreetMap/Nominatim geocoded",
            address=address.caption,
            text=full,
            results=len(results),
        )
        for result in results:
            yield result


def xref_geocode(dataset: Dataset, resolver: Resolver):
    context = Context(dataset)
    db = Database(dataset, resolver)
    loader = db.view(dataset)

    nodes: Dict[str, Dict[str, str]] = {}
    entities: Dict[str, Set[str]] = {}
    try:
        for entity in loader:
            if not entity.schema.is_a("Address"):
                continue
            # log.info("Dedupe", address=entity.caption)
            for result in query_nominatim(context, entity):
                # osm_id = result.get("osm_id")
                osm_id = result.get("display_name")
                if osm_id not in entities:
                    entities[osm_id] = set()
                    nodes[osm_id] = {
                        "name": result.get("display_name"),
                        "importance": result.get("importance"),
                        "forms": set(),
                    }

                entities[osm_id].add(entity.id)
                nodes[osm_id]["forms"].add(entity.caption)
                # context.pprint(result)
    except KeyboardInterrupt:
        pass

    resolver.prune()
    for osm_id, ids in entities.items():
        if len(ids) < 2:
            continue
        data = nodes[osm_id]
        for (a, b) in combinations(ids, 2):
            if not resolver.check_candidate(a, b):
                continue

            judgement = resolver.get_judgement(a, b)
            if judgement == Judgement.NO_JUDGEMENT:
                resolver.suggest(a, b, data["importance"])
                log.info("Suggested match", address=data["name"], forms=data["forms"])

    resolver.save()
