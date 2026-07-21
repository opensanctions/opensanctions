from functools import lru_cache
from collections.abc import Iterable, Mapping

from followthemoney.schema import Schema

from zavod.constants import ANALYZER_DATASETS
from zavod.entity import Entity
from zavod.store import View

SUPPORTING_SCHEMATA = {
    "Address",
    "Analyzable",
    "Identification",
    "Sanction",
}


def is_analyzer_stub(entity: Entity) -> bool:
    """Return whether the entity consists only of analyzer-emitted statements.

    Such entities are derived annotations (e.g. a bare topic patch) with no
    source data of their own — there is nothing to match on, so enrichers
    skip them as subjects.
    """
    return entity.datasets.issubset(ANALYZER_DATASETS)


@lru_cache(maxsize=None)
def is_supporting_schema(schema: Schema) -> bool:
    """Schemata that don't carry risk topics themselves but appear in expansion
    as attachments or context around risk targets — Addresses, Documents (Article,
    Image, PlainText, ...), Notes and other Analyzables, and non-edge Intervals
    like Sanction, Passport, Identification.
    """
    return any(schema.is_a(schema_name) for schema_name in SUPPORTING_SCHEMATA)


def endpoint_ids(entity: Entity) -> set[str]:
    """Get the entity IDs joined by an edge."""
    endpoint_ids: set[str] = set()
    if entity.schema.source_prop is not None:
        endpoint_ids.update(entity.get(entity.schema.source_prop.name))
    if entity.schema.target_prop is not None:
        endpoint_ids.update(entity.get(entity.schema.target_prop.name))
    return endpoint_ids


def _is_publishable(entity_id: str, view: View, enrich_topics: frozenset[str]) -> bool:
    canonical_id = view.store.linker.get_canonical(entity_id)
    entity = view.get_entity(canonical_id)
    if entity is None:
        return False
    if is_supporting_schema(entity.schema):
        return True
    return bool(enrich_topics.intersection(entity.get("topics", quiet=True)))


def check_publishability(
    expanded: Iterable[Entity], subject_view: View, enrich_topics: frozenset[str]
) -> dict[str, bool]:
    """Look up publishability once per entity ID that will need it.

    Non-edge supporting entities in the expansion are publishable by virtue of
    their schema, not due to risk topics, so they are seeded into the returned
    map without a lookup in the subject view.

    Non-supporting entities (the more common case - entities related via e.g.
    Ownership, Family) are looked up in the subject view where graph analyzer
    could have added topics.

    Edges (supporting and risk-connecting) are publishable if all their endpoints
    are publishable.
    """
    publishable: dict[str, bool] = {}
    ids_to_check: set[str] = set()
    for entity in expanded:
        if entity.schema.edge:
            ids_to_check.update(endpoint_ids(entity))
        else:
            assert entity.id is not None
            if is_supporting_schema(entity.schema):
                publishable[entity.id] = True
            else:
                ids_to_check.add(entity.id)
    for eid in ids_to_check:
        if eid not in publishable:
            publishable[eid] = _is_publishable(eid, subject_view, enrich_topics)
    return publishable


def should_promote(entity: Entity, publishable: Mapping[str, bool]) -> bool:
    """Publish non-edges iff the map says so (supporting schemata were seeded
    True, risk targets need a topic) and edges iff every endpoint is itself
    publishable."""
    if entity.schema.edge:
        endpoints = endpoint_ids(entity)
        if not endpoints:
            return False
        return all(publishable.get(eid, False) for eid in endpoints)
    assert entity.id is not None
    return publishable.get(entity.id, False)
