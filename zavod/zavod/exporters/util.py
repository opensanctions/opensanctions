from zavod.entity import Entity


def public_url(entity: Entity) -> str:
    return f"https://opensanctions.org/entities/{entity.id}/"