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
            # This is super unrolled in order to make it fast, its called
            # a lot during data exports. We shouldn't re-use this function
            # code in less perf critical contexts.
            stmts = entity._statements[prop.name]
            if len(stmts) < 2:
                continue
            values_in = tuple({s.value for s in stmts})
            if len(values_in) < 2:
                continue
            values = remove_prefix_date_values(values_in)
            if prop.name in PROV_MAX_DATES:
                values = (max(values),)
            elif prop.name in PROV_MIN_DATES:
                values = (min(values),)

            for stmt in list(stmts):
                if stmt.value not in values:
                    entity._statements[prop.name].remove(stmt)
    return entity


@cache
def remove_prefix_date_values(values: Tuple[str]) -> Iterable[str]:
    """See ``remove_prefix_dates``."""
    kept: List[str] = []
    values_list = sorted(values, reverse=True)
    for index, value in enumerate(values_list):
        if index > 0:
            longer = values_list[index - 1]
            if longer.startswith(value):
                continue
        kept.append(value)
    return kept


def assemble(entity: Entity) -> Entity:
    """Perform some user-facing cleanup when exporting the entity."""
    entity = simplify_dates(entity)
    return entity
