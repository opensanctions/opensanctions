import structlog
from itertools import combinations

from opensanctions.core.dataset import Dataset
from opensanctions.core.context import Context
from opensanctions.core.loader import Database
from opensanctions.core.entity import Entity
from opensanctions.core.http import get_session

log = structlog.getLogger(__name__)
NOMINATIM = "https://nominatim.openstreetmap.org/search.php"
EXPIRE_CACHE = 84600 * 200


def query_nominatim(address: Entity):
    session = get_session()
    params = {
        "q": address.first("full"),
        "countrycodes": address.get("country"),
        "format": "jsonv2",
        "accept-language": "en",
        "addressdetails": 1,
    }
    res = session.request("GET", NOMINATIM, params=params, expire_after=EXPIRE_CACHE)
    results = res.json()
    if not res.from_cache:
        log.info(
            "OpenStreetMap/Nominatim geocoded",
            address=address.caption,
            results=len(results),
        )
    for result in results:
        yield result


def xref_geocode(dataset: Dataset):
    context = Context(dataset)
    resolver = context.resolver
    db = Database(dataset, resolver)
    loader = db.view(dataset)

    nodes = {}
    entities = {}
    try:
        for entity in loader:
            if not entity.schema.is_a("Address"):
                continue
            # log.info("Dedupe", address=entity.caption)
            for result in query_nominatim(entity):
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
            resolver.suggest(a, b, data["importance"])
        log.info("Suggested match", address=data["name"], forms=data["forms"])
    resolver.save()
