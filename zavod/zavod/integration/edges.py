"""
Deduplicate edge entities in the graph.

Rules implemented:

- Same source, target, schema and temporal extent (and extra keys specified for some schemata)
- Merge edges with same temporal start and no end, into one of the edges with temporal end
  - Only for allow-listed schemata - see that for why

Rules explored but likely problematic without further investigation:

- Merge edges without temporal extent into one of the edges with temporal extent
  - Family - more specific fiancee might end less specific spouse prematurely.
  - Directorship - might merge "is signatory for" without temporal extent into "is director of" with temporal extent

This could also propose candidates for human decisions.
"""

from collections import defaultdict
from typing import Dict, List, NamedTuple, Optional, Set, Tuple

from followthemoney.property import Property
from followthemoney.schema import Schema
from nomenklatura import Resolver
from nomenklatura.judgement import Judgement
from nomenklatura.resolver import Identifier

from zavod.entity import Entity
from zavod.logs import get_logger
from zavod.store import View

log = get_logger(__name__)
Temp = Optional[Tuple[Property, str]]


class Key(NamedTuple):
    source: Identifier
    target: Identifier
    schema: Schema
    temporal_start: Temp
    temporal_end: Temp
    percentage: Optional[Tuple[str, ...]] = None
    sharesCount: Optional[Tuple[str, ...]] = None
    sharesValue: Optional[Tuple[str, ...]] = None
    sharesCurrency: Optional[Tuple[str, ...]] = None
    amount: Optional[Tuple[str, ...]] = None
    currency: Optional[Tuple[str, ...]] = None
    amountUsd: Optional[Tuple[str, ...]] = None
    role: Optional[Tuple[str, ...]] = None


# List schemata in decreasing order of specificity. We use this ordering to pick the most specific matching schema
EXTRA_KEY_PROPS = {
    # Don't mix different values/units of ownership
    "Ownership": [
        "percentage",
        "sharesCount",
        "sharesValue",
        "sharesCurrency",
    ],
    # It's probably nice to be able to keep different UnknownLinks separate
    # - especially ones without temporal extent -
    # considering role is basically all we have and they could be quite different things
    "UnknownLink": ["role"],
    "Value": ["amount", "currency", "amountUsd"],  # Don't mix different values/units
    # "Membership": ["role"],
    # Directorship": ["role"]  # When the temporal extent matches, it seems safe to ignore role
    # "Employment": ["role"]  # Similar to Directorship
    # "Associate": ["relationship"],
    # "Representation": ["role"],
    # "Family": ["relationship"],  # when the temporal extent matches, we can ignore relationship.
    # "Succession": []  # How many ways and times can one entity succeed another in this data?
}
EXTRA_KEY_PROPS_COMMON_START = EXTRA_KEY_PROPS.copy()
# Prevents e.g. merging presidency which ends with directorship which continues
EXTRA_KEY_PROPS_COMMON_START["Directorship"] = ["role"]
ALLOW_COMMON_START = {
    "Occupancy",  # Cleans up cases where one source has an end date and another source doesn't.
    "Directorship",  # With the caveat of also considering role
    "Ownership",  # Merges e.g. directly consolidated by, with ultimately consolidated by
}


def get_vertices(entity: Entity) -> Optional[Tuple[Identifier, Identifier]]:
    assert entity.schema.source_prop and entity.schema.target_prop
    sources = [Identifier.get(s) for s in entity.get(entity.schema.source_prop)]
    targets = [Identifier.get(t) for t in entity.get(entity.schema.target_prop)]
    if not sources or not targets:
        return None
    if len(set(sources).union(targets)) > 2:
        log.warning(
            "Multi-ended edge", entity=entity.id, sources=sources, targets=targets
        )
        return None
    # Make source and target consistent for non-directed edges
    source = min(sources)
    target = max(targets)
    if not entity.schema.edge_directed:
        source, target = min((source, target)), max((source, target))

    return source, target


def make_key(
    connected: Tuple[Identifier, Identifier],
    entity: Entity,
    extra_props_rules: Dict[str, List[str]],
    blank_end: bool = False,
) -> Key:
    extra_props: Set[str] = set()
    for schema in extra_props_rules:
        if entity.schema.is_a(schema):
            extra_props.update(extra_props_rules[schema])
            break

    key = Key(
        connected[0],
        connected[1],
        entity.schema,
        entity.temporal_start,
        entity.temporal_end if not blank_end else None,
    )
    for prop in extra_props:
        value = tuple(sorted(entity.get(prop)))
        if value:
            assert prop not in {
                "source",
                "target",
                "schema",
                "temporal_start",
                "temporal_end",
            }
            key = key._replace(**{prop: value})  # type: ignore
    return key


def group_common_start(
    groups: Dict[Key, List[str]], common_start: Dict[Key, Set[Key]]
) -> None:
    """
    Adds IDs to a group they can be merged with.

    If there's a key with only temporal start, and exactly one key with start and end,
    merge with that one.

    If there are more than one non-blank end keys, we don't know which is the correct one
    to merge with, so we don't merge any.
    """
    group_count = 0

    for common_start_key, values in common_start.items():
        if common_start_key.schema.name not in ALLOW_COMMON_START:
            log.warning(
                "Skipping common start grouping for unapproved schema %s (source=%s target=%s)"
                % (
                    common_start_key.schema.name,
                    common_start_key.source.id,
                    common_start_key.target.id,
                )
            )
            continue
        if len(values) != 2:
            continue
        no_end_keys = [v for v in values if v.temporal_end is None]
        if len(no_end_keys) == 0:
            continue
        assert len(no_end_keys) == 1
        values.remove(no_end_keys[0])
        other_key = values.pop()
        other_id = groups[other_key][0]
        groups[no_end_keys[0]].append(other_id)
        group_count += 1
    log.info("%d edges to be merged with a group with a common start date" % group_count)


def group_edges(resolver: Resolver[Entity], view: View) -> Dict[Key, List[str]]:
    groups: Dict[Key, List[str]] = defaultdict(list)
    common_start: Dict[Key, Set[Key]] = defaultdict(set)

    for idx, entity in enumerate(view.entities()):
        if idx > 0 and idx % 10000 == 0:
            log.info("Keyed %s entities..." % idx)

        if (
            not entity.schema.edge
            or entity.schema.source_prop is None
            or entity.schema.target_prop is None
            or entity.id is None
        ):
            continue

        connected = get_vertices(entity)
        if connected is None:
            continue

        key = make_key(connected, entity, EXTRA_KEY_PROPS)
        groups[key].append(entity.id)

        if entity.temporal_start is not None:
            common_start_key = make_key(
                connected,
                entity,
                EXTRA_KEY_PROPS_COMMON_START,
                blank_end=True,
            )
            common_start[common_start_key].add(key)

    group_common_start(groups, common_start)
    return groups


def merge_groups(
    resolver: Resolver[Entity], view: View, groups: Dict[Key, List[str]]
) -> None:
    merged_count = 0
    cluster_count = 0

    for key, values in groups.items():
        if len(values) == 1:
            continue

        first_id = values[0]
        merged_count += 1

        canonical = resolver.get_canonical(first_id)
        for other_id in values[1:]:
            other_canon = resolver.get_canonical(other_id)
            if other_canon == canonical:
                continue
            other = view.get_entity(other_id)

            log.info("Merge edge: %s (%s -> %s)" % (other, other_canon, canonical))
            canonical = resolver.decide(
                canonical,
                other_canon,
                judgement=Judgement.POSITIVE,
                user="edge-dedupe",
            ).id
            merged_count += 1
        cluster_count += 1
    log.info("Merged %s edge entities into %s clusters" % (merged_count, cluster_count))


def dedupe_edges(resolver: Resolver[Entity], view: View) -> None:
    groups = group_edges(resolver, view)
    merge_groups(resolver, view, groups)
