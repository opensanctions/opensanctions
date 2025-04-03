from typing import Dict, Tuple, Union, List

from nomenklatura import Resolver
from nomenklatura.resolver import Identifier
from nomenklatura.judgement import Judgement
from followthemoney.schema import Schema
from followthemoney.property import Property

from zavod.logs import get_logger
from zavod.entity import Entity
from zavod.store import View

log = get_logger(__name__)
Temp = Union[None, Tuple[Property, str]]
Key = Tuple[Identifier, Identifier, Schema, Temp, Temp]


def group_relations(resolver: Resolver, view: View) -> Dict[Key, List[Entity]]:
    groups: Dict[Key, List[str]] = {}

    for idx, entity in enumerate(view.entities()):
        if (
            not entity.schema.edge
            or entity.schema.source_prop is None
            or entity.schema.target_prop is None
            or entity.id is None
        ):
            continue

        resolver.explode(entity.id)

        if idx > 0 and idx % 10000 == 0:
            log.info("Keyed %s entities..." % idx)

        sources = [Identifier.get(s) for s in entity.get(entity.schema.source_prop)]
        targets = [Identifier.get(t) for t in entity.get(entity.schema.target_prop)]
        if len(set(sources).union(targets)) > 2:
            log.warning(
                "Multi-ended edge", entity=entity.id, sources=sources, targets=targets
            )
            continue
        source = min(sources)
        target = max(targets)
        if not entity.schema.edge_directed:
            source, target = min((source, target)), max((source, target))

        key = (
            source,
            target,
            entity.schema,
            entity.temporal_start,
            entity.temporal_end,
        )

        if key not in groups:
            groups[key] = []
        groups[key].append(entity.id)
    return groups


def merge_groups(resolver: Resolver, view: View, groups: Dict[Key, List[str]]) -> None:
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
    log.info("Merged %s relations into %s clusters" % (merged_count, cluster_count))


def dedupe_relations(resolver: Resolver, view: View) -> None:
    groups = group_relations(resolver, view)
    merge_groups(resolver, view, groups)
