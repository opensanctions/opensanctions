from functools import cache
from typing import cast, Any, Dict, Optional
from nomenklatura.entity import CE

from zavod.context import Context
from zavod import settings
from zavod.meta import get_multi_dataset
from zavod.store import get_view


@cache
def get_position(context: Context, entity_id: str) -> Optional[Dict[str, Any]]:
    url = f"{settings.API_URL}/positions/{entity_id}"
    headers = {"authorization": settings.API_KEY}
    res = context.http.get(url, headers=headers)

    if res.status_code == 200:
        return res.json()
    elif res.status_code == 404:
        return None
    else:
        res.raise_for_status()


def analyze_position(context: Context, entity: CE) -> None:
    entity_ids = entity.referents
    entity_ids.add(entity.id)
    if not entity_ids:
        return
    for entity_id in entity_ids:
        data = get_position(context, entity_id)
        if data is None:
            continue
        proxy = context.make("Position")
        proxy.id = entity_id
        proxy.add("topics", data["topics"])
        if proxy.get("topics"):
            context.emit(proxy)


def crawl(context: Context) -> None:
    view = get_view(get_multi_dataset(context.dataset.inputs))
    
    for entity_idx, entity in enumerate(view.entities()):
        if entity_idx > 0 and entity_idx % 1000 == 0:
            context.cache.flush()
        
        if entity.schema.is_a("Position"):
            analyze_position(context, entity)
            
        