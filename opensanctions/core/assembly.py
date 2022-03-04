from collections import defaultdict
from typing import Dict, List, Optional
import Levenshtein
from itertools import combinations
from normality import latinize_text
from followthemoney.types import registry
from followthemoney.helpers import remove_prefix_dates
from followthemoney.helpers import simplify_provenance

from opensanctions.core.entity import Entity


def _pick_name(names: List[str], all_names: List[str]) -> Optional[str]:
    for name in names:
        latin = latinize_text(name)
        if latin is not None:
            all_names.append(latin.title())

    scores: Dict[str, int] = defaultdict(int)
    for pair in combinations(all_names, 2):
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
            name = _pick_name(names, all_names)
            if name in names:
                names.remove(name)
            entity.set("name", name)
            entity.add("alias", names)
    return entity


def assemble(entity: Entity) -> Entity:
    """Perform some user-facing cleanup when exporting the entity."""
    entity = simplify_provenance(entity)
    entity = remove_prefix_dates(entity)
    return name_entity(entity)
