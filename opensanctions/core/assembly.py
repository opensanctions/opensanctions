import Levenshtein
from functools import cache
from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Tuple
from itertools import combinations
from normality import latinize_text
from followthemoney.types import registry

from opensanctions.core.entity import Entity

PROV_MIN_DATES = ("createdAt", "authoredAt", "publishedAt")
PROV_MAX_DATES = ("modifiedAt", "retrievedAt")


@cache
def _pick_name(names: Tuple[str], all_names: Tuple[str]) -> Optional[str]:
    candidates: List[str] = []
    for name in all_names:
        candidates.append(name)
        latin = latinize_text(name)
        if latin is not None:
            candidates.append(latin.title())

    scores: Dict[str, int] = defaultdict(int)
    for pair in combinations(candidates, 2):
        left, right = sorted(pair)
        dist = Levenshtein.distance(left[:128], right[:128])
        scores[left] += dist
        scores[right] += dist

    for cand, _ in sorted(scores.items(), key=lambda x: x[1]):
        if cand in names:
            return cand
    return None


def name_entity(entity: Entity) -> Entity:
    """If an entity has multiple names, pick the most central one
    and set all the others as aliases. This is awkward given that
    names are not special and may not always be the caption."""
    if entity.schema.is_a("Thing"):
        names = entity.get("name")
        if len(names) > 1:
            all_names = entity.get_type_values(registry.name)
            name = _pick_name(tuple(names), tuple(all_names))
            if name in names:
                names.remove(name)
            entity.set("name", name, cleaned=True)
            entity.add("alias", names, cleaned=True)
    return entity


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
    entity = name_entity(entity)
    return entity
