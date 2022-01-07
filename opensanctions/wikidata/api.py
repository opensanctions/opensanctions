# https://www.mediawiki.org/wiki/Wikibase/API
# https://www.wikidata.org/w/api.php?action=help&modules=wbgetentities
import random
import structlog
from typing import Any, Dict, Optional
from asyncstdlib.functools import cache

from opensanctions import settings
from opensanctions.core import Context
from opensanctions.core.http import get_session
from opensanctions.wikidata.lang import pick_obj_lang

WD_API = "https://www.wikidata.org/w/api.php"
log = structlog.getLogger(__name__)
EXPIRE_AFTER_LONG = 84600 * 180


async def wikibase_getentities(context: Context, ids, expire_long=False, **kwargs):
    expire = EXPIRE_AFTER_LONG if expire_long else settings.CACHE_EXPIRE
    expire = random.randint(expire * 0.8, expire * 1.2)
    params = {**kwargs, "format": "json", "ids": ids, "action": "wbgetentities"}
    context.http_concurrency = 2
    return await context.fetch_json(WD_API, params=params)
    # session = get_session()
    # resp = session.request("GET", WD_API, params=params, expire_after=expire)
    # if resp.ok:
    #     return resp.json()


async def get_entity(context: Context, qid: str) -> Optional[Dict[str, Any]]:
    data = await wikibase_getentities(context, qid)
    return data.get("entities", {}).get(qid)


@cache
async def get_label(context: Context, qid: str) -> Optional[str]:
    data = await wikibase_getentities(context, qid, props="labels", expire_long=True)
    entity = data.get("entities", {}).get(qid)
    # pprint(entity)
    return pick_obj_lang(entity.get("labels", {}))
