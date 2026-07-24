from collections.abc import Iterable, Mapping

from zavod.constants import ANALYZER_DATASETS
from zavod.entity import Entity
from zavod.store import View


def is_analyzer_stub(entity: Entity) -> bool:
    """Return whether the entity consists only of analyzer-emitted statements.

    Such entities are derived annotations (e.g. a bare topic patch) with no
    source data of their own — there is nothing to match on, so enrichers
    skip them as subjects.
    """
    return entity.datasets.issubset(ANALYZER_DATASETS)


def endpoint_ids(entity: Entity) -> set[str]:
    """Get the entity IDs joined by an edge."""
    endpoint_ids: set[str] = set()
    if entity.schema.source_prop is not None:
        endpoint_ids.update(entity.get(entity.schema.source_prop.name))
    if entity.schema.target_prop is not None:
        endpoint_ids.update(entity.get(entity.schema.target_prop.name))
    return endpoint_ids


def has_enrich_topic(entity_id: str, view: View, enrich_topics: frozenset[str]) -> bool:
    """Check whether an entity carries a topic that justifies publication."""
    canonical_id = view.store.linker.get_canonical(entity_id)
    entity = view.get_entity(canonical_id)
    if entity is None:
        return False
    return bool(enrich_topics.intersection(entity.get("topics")))


def check_enrich_topics(
    expanded: Iterable[Entity], view: View, enrich_topics: frozenset[str]
) -> dict[str, bool]:
    """Look up publication topics once per entity ID in an expansion."""
    ids_to_check: set[str] = set()
    for entity in expanded:
        if entity.schema.edge:
            ids_to_check.update(endpoint_ids(entity))
        else:
            assert entity.id is not None
            ids_to_check.add(entity.id)
    return {eid: has_enrich_topic(eid, view, enrich_topics) for eid in ids_to_check}


def should_promote(entity: Entity, topic_matches: Mapping[str, bool]) -> bool:
    """Publish nodes with a topic and edges whose endpoints all have topics."""
    if entity.schema.edge:
        endpoints = endpoint_ids(entity)
        if not endpoints:
            return False
        return all(topic_matches.get(entity_id, False) for entity_id in endpoints)
    assert entity.id is not None
    return topic_matches.get(entity.id, False)
