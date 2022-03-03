# https://www.mediawiki.org/wiki/Wikibase/API
# https://www.wikidata.org/w/api.php?action=help&modules=wbgetentities
import json
import structlog
from datetime import timedelta
from typing import Any, Dict, Optional

from opensanctions.core import Context
from opensanctions.core.db import engine_read
from opensanctions.core.cache import all_cached
from opensanctions.wikidata.lang import pick_obj_lang

WD_API = "https://www.wikidata.org/w/api.php"
LABELS: Dict[str, Optional[str]] = {}
CACHE_LABELS = 100
log = structlog.getLogger(__name__)


def wikibase_getentities(context: Context, ids, cache_days=None, **kwargs):
    params = {**kwargs, "format": "json", "ids": ids, "action": "wbgetentities"}
    return context.fetch_json(WD_API, params=params, cache_days=cache_days)


def get_entity(context: Context, qid: str) -> Optional[Dict[str, Any]]:
    data = wikibase_getentities(
        context,
        qid,
        cache_days=14,
    )
    return data.get("entities", {}).get(qid)


def get_label(context: Context, qid: str) -> Optional[str]:
    if qid in LABELS:
        return LABELS[qid]
    data = wikibase_getentities(
        context,
        qid,
        cache_days=CACHE_LABELS,
        props="labels",
    )
    entity = data.get("entities", {}).get(qid)
    # pprint(entity)
    label = pick_obj_lang(entity.get("labels", {}))
    LABELS[qid] = label
    return label


def load_labels(context: Context) -> None:
    context.log.info("Loading QID labels...")
    with engine_read() as conn:
        like = "https://www.wikidata.org/w/api.php%props=labels%"
        max_age = timedelta(days=CACHE_LABELS)
        for resp in all_cached(conn, like, max_age):
            data = json.loads(resp)
            for qid, entity in data.get("entities", {}).items():
                label = pick_obj_lang(entity.get("labels", {}))
                LABELS[qid] = label
