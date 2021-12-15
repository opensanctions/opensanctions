# https://www.mediawiki.org/wiki/Wikibase/API
# https://www.wikidata.org/w/api.php?action=help&modules=wbgetentities

# snak = some notation about knowledge
from functools import cache
from typing import Any, Dict, Optional

from opensanctions import settings
from opensanctions.core.http import get_session
from opensanctions.wikidata.lang import pick_obj_lang

WD_API = "https://www.wikidata.org/w/api.php"


def wikibase_getentities(ids, expire_long=False, **kwargs):
    expire = settings.CACHE_EXPIRE * 50 if expire_long else settings.CACHE_EXPIRE
    params = {**kwargs, "format": "json", "ids": ids, "action": "wbgetentities"}
    session = get_session()
    resp = session.request("GET", WD_API, params=params, expire_after=expire)
    if resp.ok:
        return resp.json()


def get_entity(qid: str) -> Optional[Dict[str, Any]]:
    data = wikibase_getentities(qid)
    return data.get("entities", {}).get(qid)


@cache
def get_label(qid: str) -> Optional[str]:
    data = wikibase_getentities(qid, props="labels", expire_long=True)
    entity = data.get("entities", {}).get(qid)
    # pprint(entity)
    return pick_obj_lang(entity.get("labels", {}))


if __name__ == "__main__":
    for i in range(100):
        print(get_label("Q42"))
