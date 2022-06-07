from functools import cache
from typing import Iterable, List, Tuple
from followthemoney.types import registry

from opensanctions.core.entity import Entity

PROV_MIN_DATES = ("createdAt", "authoredAt", "publishedAt")
PROV_MAX_DATES = ("modifiedAt", "retrievedAt")


def simplify_dates(entity: Entity) -> Entity:
    """If an entity has multiple values for a date field, you may
    want to remove all those that are prefixes of others. For example,
    if a Person has both a birthDate of 1990 and of 1990-05-01, we'd
    want to drop the mention of 1990."""
    for prop in entity.iterprops():
        if prop.type == registry.date:
            dates = tuple(entity.pop(prop))
            values = remove_prefix_date_values(dates)
            if prop.name in PROV_MAX_DATES:
                entity.unsafe_add(prop, max(values), cleaned=True)
            elif prop.name in PROV_MIN_DATES:
                entity.unsafe_add(prop, min(values), cleaned=True)
            else:
                for value in values:
                    entity.unsafe_add(prop, value, cleaned=True)
    return entity


@cache
def remove_prefix_date_values(values: Tuple[str]) -> Iterable[str]:
    """See ``remove_prefix_dates``."""
    if len(values) < 2:
        return values
    kept: List[str] = []
    values = sorted(values, reverse=True)
    for index, value in enumerate(values):
        if index > 0:
            longer = values[index - 1]
            if longer.startswith(value):
                continue
        kept.append(value)
    return kept


def assemble(entity: Entity) -> Entity:
    """Perform some user-facing cleanup when exporting the entity."""
    entity = simplify_dates(entity)
    return entity
