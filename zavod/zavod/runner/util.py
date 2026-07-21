from functools import lru_cache
from typing import Iterable, Mapping

from followthemoney.schema import Schema

from zavod.entity import Entity
from zavod.store import View

SUPPORTING_SCHEMATA = {
    "Address",
    "Analyzable",
    "Identification",
    "Sanction",
}


@lru_cache(maxsize=None)
def is_supporting_schema(schema: Schema) -> bool:
    """Schemata that don't carry risk topics themselves but appear in expansion
    as attachments or context around risk targets — Addresses, Documents (Article,
    Image, PlainText, ...), Notes and other Analyzables, and non-edge Intervals
    like Sanction, Passport, Identification. Under topic gating these are always
    published when expansion reaches them; only risk targets (LegalEntity/Asset
    descendants, Position, CryptoWallet) require a topic.
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
    expanded: Iterable[Entity], view: View, enrich_topics: frozenset[str]
) -> dict[str, bool]:
    """Look up publishability once per entity ID that will need it.

    We need a lookup for every edge endpoint (to gate the edge) and for every
    non-edge risk target (to gate the node). Non-edge supporting schemata are
    always publishable, so we skip the lookup for them.
    """
    ids_to_check: set[str] = set()
    for entity in expanded:
        if entity.schema.edge:
            ids_to_check.update(endpoint_ids(entity))
        else:
            assert entity.id is not None
            ids_to_check.add(entity.id)
    return {eid: _is_publishable(eid, view, enrich_topics) for eid in ids_to_check}


def should_promote(entity: Entity, publishable: Mapping[str, bool]) -> bool:
    """Publish supporting non-edges always, risk-target non-edges iff they carry a
    topic, and edges iff every endpoint is itself publishable."""
    if entity.schema.edge:
        endpoints = endpoint_ids(entity)
        if not endpoints:
            return False
        return all(publishable.get(eid, False) for eid in endpoints)
    assert entity.id is not None
    return publishable.get(entity.id, False)
