# https://www.mediawiki.org/wiki/Wikibase/API
# https://www.wikidata.org/w/api.php?action=help&modules=wbgetentities
import json
import structlog
from functools import cache
from datetime import timedelta
from typing import Any, Dict, Optional

from opensanctions import settings
from opensanctions.core import Context
from opensanctions.core.db import engine_read
from opensanctions.core.cache import all_cached, Cache, randomize_cache
from opensanctions.wikidata.lang import pick_obj_lang
from opensanctions.util import normalize_url

WD_API = "https://www.wikidata.org/w/api.php"
CACHED: Dict[str, Cache] = {}
log = structlog.getLogger(__name__)


def wikibase_getentities(context: Context, ids, cache_days=None, **kwargs):
    params = {**kwargs, "format": "json", "ids": ids, "action": "wbgetentities"}
    full_url = normalize_url(WD_API, params)
    cached = get_cached(full_url, cache_days)
    if cached is not None:
        return cached
    return context.fetch_json(full_url, cache_days=cache_days)


def get_entity(context: Context, qid: str) -> Optional[Dict[str, Any]]:
    data = wikibase_getentities(
        context,
        qid,
        cache_days=14,
    )
    return data.get("entities", {}).get(qid)


@cache
def get_label(context: Context, qid: str) -> Optional[str]:
    data = wikibase_getentities(
        context,
        qid,
        cache_days=100,
        props="labels",
    )
    entity = data.get("entities", {}).get(qid)
    label = pick_obj_lang(entity.get("labels", {}))
    return label


def load_api_cache(context: Context) -> None:
    context.log.info("Loading wikidata API cache...")
    with engine_read() as conn:
        like = "https://www.wikidata.org/w/api.php%"
        # max_age = timedelta(days=CACHE_LABELS)
        for cache in all_cached(conn, like):
            CACHED[cache["url"]] = cache
            # data = json.loads(resp)
            # for qid, entity in data.get("entities", {}).items():
            #     label = pick_obj_lang(entity.get("labels", {}))
            #     # LABELS[qid] = label


def get_cached(url: str, cache_days: int):
    cache = CACHED.get(url)
    if cache is None:
        return None
    cache_cutoff = settings.RUN_TIME - randomize_cache(cache_days)
    if cache["timestamp"] < cache_cutoff:
        print("OUTDATED", cache["timestamp"], cache_days, cache_cutoff)
        return None
    text = cache.get("text")
    if text is None:
        return None
    return json.loads(text)
