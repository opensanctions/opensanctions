# https://www.mediawiki.org/wiki/Wikibase/API
# https://www.wikidata.org/w/api.php?action=help&modules=wbgetentities
import structlog
from typing import Any, Dict, Optional
from asyncstdlib.functools import cache

from opensanctions.core import Context
from opensanctions.wikidata.lang import pick_obj_lang

WD_API = "https://www.wikidata.org/w/api.php"
log = structlog.getLogger(__name__)
EXPIRE_AFTER_LONG = 84600 * 180


async def wikibase_getentities(context: Context, ids, cache_days=None, **kwargs):
    params = {**kwargs, "format": "json", "ids": ids, "action": "wbgetentities"}
    context.http_concurrency = 2
    return await context.fetch_json(WD_API, params=params, cache_days=cache_days)


async def get_entity(context: Context, qid: str) -> Optional[Dict[str, Any]]:
    data = await wikibase_getentities(
        context,
        qid,
        cache_days=7,
    )
    return data.get("entities", {}).get(qid)


@cache
async def get_label(context: Context, qid: str) -> Optional[str]:
    data = await wikibase_getentities(
        context,
        qid,
        cache_days=100,
        props="labels",
        expire_long=True,
    )
    entity = data.get("entities", {}).get(qid)
    # pprint(entity)
    return pick_obj_lang(entity.get("labels", {}))
