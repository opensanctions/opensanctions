from functools import lru_cache
from typing import Iterable, Mapping

from followthemoney import registry
from followthemoney.property import Property
from followthemoney.schema import Schema

from zavod.context import Context
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
    # Match BaseEnricher._filter_entity's topic extraction (any topic-typed property).
    return bool(enrich_topics.intersection(entity.get_type_values(registry.topic)))


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


def prune_unpublishable_references(
    context: Context, entity: Entity, publishable: Mapping[str, bool]
) -> list[tuple[Property, str]]:
    """Drop references from a non-edge entity to entities that will not be
    published (e.g. a security's issuer without a risk topic), so that the
    published entity doesn't contain dangling references. Edges are only
    published when all their endpoints are (see ``should_promote``), so they
    are left untouched.

    Returns the removed ``(prop, referenced_id)`` pairs so the caller can
    re-emit them as external — keeping the relationship visible to the graph
    analyzer (which reads the external view and may tag the referenced entity,
    making it publishable on a later run) without the exporter seeing it."""
    pruned: list[tuple[Property, str]] = []
    if entity.schema.edge:
        return pruned
    for prop in list(entity.iterprops()):
        if prop.type != registry.entity:
            continue
        for other_id in entity.get(prop):
            if not publishable.get(other_id, False):
                entity.remove(prop, other_id)
                pruned.append((prop, other_id))
                context.log.info(
                    "Demoting reference to unpublishable entity to external",
                    entity_id=entity.id,
                    prop=prop.name,
                    ref=other_id,
                )
    return pruned
