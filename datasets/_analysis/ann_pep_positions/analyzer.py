from functools import cache
from typing import Any, Dict, Optional

from zavod import Context, Entity
from zavod import settings
from zavod.meta import get_multi_dataset
from zavod.store import get_view


@cache
def get_position(context: Context, entity_id: str) -> Optional[Dict[str, Any]]:
    url = f"{settings.OPENSANCTIONS_API_URL}/positions/{entity_id}"
    headers = {"authorization": settings.OPENSANCTIONS_API_KEY}
    res = context.http.get(url, headers=headers)
    if res.status_code == 404:
        return None
    res.raise_for_status()
    return res.json()


def analyze_position(context: Context, entity: Entity) -> None:
    entity_ids = entity.referents
    if entity.id is not None:
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
    position_count = 0

    for entity_idx, entity in enumerate(view.entities()):
        if entity_idx > 0 and entity_idx % 1000 == 0:
            context.log.info("Processed %s entities" % entity_idx)
            context.cache.flush()
        
        if entity.schema.is_a("Position"):
            analyze_position(context, entity)
            position_count += 1
            if position_count % 1000 == 0:
                context.log.info("Analyzed %s positions" % position_count)
                context.cache.flush()
            
        