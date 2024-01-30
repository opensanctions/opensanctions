from typing import Optional
from zavod.entity import Entity


def public_url(entity: Entity) -> Optional[str]:
    # TODO: implement check if the entity is in default, if not return None
    return f"https://opensanctions.org/entities/{entity.id}/"