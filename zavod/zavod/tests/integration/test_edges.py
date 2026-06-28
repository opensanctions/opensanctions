from typing import Optional

import pytest
from nomenklatura import Resolver, Store
from nomenklatura.store import SimpleMemoryStore

from zavod.entity import Entity
from zavod.integration.edges import dedupe_edges
from zavod.meta.dataset import Dataset


@pytest.fixture
def store(resolver: Resolver, testdataset1: Dataset) -> SimpleMemoryStore:
    return SimpleMemoryStore(testdataset1, resolver)


def add_entities(store: Store, entities: list[Entity]):
    writer = store.writer()
    for entity in entities:
        writer.add_entity(entity)
    writer.flush()


def e(
    dataset: Dataset,
    schema: str,
    id: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    properties: Optional[dict[str, str | list[str] | None]] = None,
) -> Entity:
    assert type(start) is str or start is None
    assert type(end) is str or end is None
    data = dict(properties or {})
    data["startDate"] = start
    data["endDate"] = end
    return Entity.from_data(
        dataset,
        {
            "schema": schema,
            "id": id,
            "properties": {
                prop: value if isinstance(value, list) else [value]
                for prop, value in data.items()
                if value is not None
            },
        },
        cleaned=False,
    )


def assert_merged(resolver: Resolver, *ids: str) -> None:
    canonicals = {resolver.get_canonical(id_) for id_ in ids}
    assert len(canonicals) == 1, canonicals


def assert_not_merged(resolver: Resolver, *ids: str) -> None:
    canonicals = {resolver.get_canonical(id_) for id_ in ids}
    assert len(canonicals) == len(ids), canonicals


def test_directed_edges_preserve_direction(store: Store, resolver: Resolver):
    entity1 = e(
        store.dataset,
        "Directorship",
        "e1",
        properties={"director": "a", "organization": "b"},
    )
    entity2 = e(
        store.dataset,
        "Directorship",
        "e2",
        properties={"director": "b", "organization": "a"},
    )
    add_entities(store, [entity1, entity2])

    dedupe_edges(resolver, store.default_view())
    assert_not_merged(resolver, "e1", "e2")


def test_undirected_edges_canonicalize_endpoints(store: Store, resolver: Resolver):
    entity1 = e(
        store.dataset,
        "UnknownLink",
        "e1",
        properties={"subject": "a", "object": "b", "role": "advisor"},
    )
    entity2 = e(
        store.dataset,
        "UnknownLink",
        "e2",
        properties={"subject": "b", "object": "a", "role": "advisor"},
    )
    add_entities(store, [entity1, entity2])

    dedupe_edges(resolver, store.default_view())
    assert_merged(resolver, "e1", "e2")


def test_multi_ended_edges_are_skipped(store: Store, resolver: Resolver):
    entity1 = e(
        store.dataset,
        "Ownership",
        "e1",
        properties={"owner": ["a", "c"], "asset": "b"},
    )
    entity2 = e(
        store.dataset,
        "Ownership",
        "e2",
        properties={"owner": "a", "asset": "b"},
    )
    add_entities(store, [entity1, entity2])

    dedupe_edges(resolver, store.default_view())
    assert_not_merged(resolver, "e1", "e2")


def test_different_schemata_do_not_merge(store: Store, resolver: Resolver):
    entity1 = e(
        store.dataset,
        "Ownership",
        "e1",
        properties={"owner": "a", "asset": "b"},
    )
    entity2 = e(
        store.dataset,
        "Directorship",
        "e2",
        properties={"director": "a", "organization": "b"},
    )
    add_entities(store, [entity1, entity2])

    dedupe_edges(resolver, store.default_view())
    assert_not_merged(resolver, "e1", "e2")


def test_same_temporal_extent_merges(store: Store, resolver: Resolver):
    entity1 = e(
        store.dataset,
        "UnknownLink",
        "e1",
        "2020-01-01",
        "2021-01-01",
        {"subject": "a", "object": "b", "role": "AAA", "description": "aaa"},
    )
    entity2 = e(
        store.dataset,
        "UnknownLink",
        "e2",
        "2020-01-01",
        "2021-01-01",
        {"subject": "a", "object": "b", "role": "AAA", "description": "bbb"},
    )
    add_entities(store, [entity1, entity2])

    dedupe_edges(resolver, store.default_view())
    assert_merged(resolver, "e1", "e2")


def test_missing_dates_are_temporally_compatible(store: Store, resolver: Resolver):
    entity1 = e(
        store.dataset, "Ownership", "e1", None, None, {"owner": "a", "asset": "b"}
    )
    entity2 = e(
        store.dataset, "Ownership", "e2", "2024", None, {"owner": "a", "asset": "b"}
    )
    add_entities(store, [entity1, entity2])

    dedupe_edges(resolver, store.default_view())
    assert_merged(resolver, "e1", "e2")


def test_partial_dates_overlap(store: Store, resolver: Resolver):
    entity1 = e(
        store.dataset,
        "Directorship",
        "e1",
        "2025",
        None,
        {"director": "a", "organization": "b", "role": "director"},
    )
    entity2 = e(
        store.dataset,
        "Directorship",
        "e2",
        "2025-10-01",
        None,
        {"director": "a", "organization": "b", "role": "director"},
    )
    add_entities(store, [entity1, entity2])

    dedupe_edges(resolver, store.default_view())
    assert_merged(resolver, "e1", "e2")


def test_incompatible_dates_do_not_merge(store: Store, resolver: Resolver):
    entity1 = e(
        store.dataset,
        "Directorship",
        "e1",
        "2025-09",
        None,
        {"director": "a", "organization": "b", "role": "director"},
    )
    entity2 = e(
        store.dataset,
        "Directorship",
        "e2",
        "2025-10-01",
        None,
        {"director": "a", "organization": "b", "role": "director"},
    )
    add_entities(store, [entity1, entity2])

    dedupe_edges(resolver, store.default_view())
    assert_not_merged(resolver, "e1", "e2")


def test_ambiguous_temporal_bridge_is_skipped(store: Store, resolver: Resolver):
    entity1 = e(
        store.dataset,
        "Directorship",
        "e1",
        "2025",
        None,
        {"director": "a", "organization": "b", "role": "director"},
    )
    entity2 = e(
        store.dataset,
        "Directorship",
        "e2",
        "2025-01-01",
        None,
        {"director": "a", "organization": "b", "role": "director"},
    )
    entity3 = e(
        store.dataset,
        "Directorship",
        "e3",
        "2025-12-31",
        None,
        {"director": "a", "organization": "b", "role": "director"},
    )
    add_entities(store, [entity1, entity2, entity3])

    dedupe_edges(resolver, store.default_view())
    assert_not_merged(resolver, "e1", "e2", "e3")


def test_ambiguous_bridge_does_not_block_unambiguous_merge(
    store: Store, resolver: Resolver
):
    # An ambiguous bridge (2025 overlaps both exact dates) and a conflicting edge
    # share a bucket with an identical, unambiguous pair. The bridge must not pull
    # the unambiguous pair into a discarded group.
    base = {"director": "a", "organization": "b", "role": "director"}
    bridge = e(store.dataset, "Directorship", "bridge", "2025", None, base)
    pair1 = e(store.dataset, "Directorship", "pair1", "2025-01-01", None, base)
    pair2 = e(store.dataset, "Directorship", "pair2", "2025-01-01", None, base)
    conflict = e(store.dataset, "Directorship", "conflict", "2025-12-31", None, base)
    add_entities(store, [bridge, pair1, conflict, pair2])

    dedupe_edges(resolver, store.default_view())
    assert_merged(resolver, "pair1", "pair2")
    assert_not_merged(resolver, "bridge", "conflict", "pair1")


def test_protected_props_match_on_shared_normalized_value(
    store: Store, resolver: Resolver
):
    entity1 = e(
        store.dataset,
        "Directorship",
        "e1",
        properties={
            "director": "a",
            "organization": "b",
            "role": "Président-directeur général",
        },
    )
    entity2 = e(
        store.dataset,
        "Directorship",
        "e2",
        properties={
            "director": "a",
            "organization": "b",
            "role": "president directeur general",
        },
    )
    add_entities(store, [entity1, entity2])

    dedupe_edges(resolver, store.default_view())
    assert_merged(resolver, "e1", "e2")


def test_protected_props_conflict_on_disjoint_values(store: Store, resolver: Resolver):
    entity1 = e(
        store.dataset,
        "UnknownLink",
        "e1",
        properties={"subject": "a", "object": "b", "role": "director"},
    )
    entity2 = e(
        store.dataset,
        "UnknownLink",
        "e2",
        properties={"subject": "a", "object": "b", "role": "signatory"},
    )
    add_entities(store, [entity1, entity2])

    dedupe_edges(resolver, store.default_view())
    assert_not_merged(resolver, "e1", "e2")


def test_empty_protected_props_do_not_block_merge(store: Store, resolver: Resolver):
    entity1 = e(
        store.dataset,
        "Directorship",
        "e1",
        properties={"director": "a", "organization": "b"},
    )
    entity2 = e(
        store.dataset,
        "Directorship",
        "e2",
        properties={"director": "a", "organization": "b", "role": "director"},
    )
    add_entities(store, [entity1, entity2])

    dedupe_edges(resolver, store.default_view())
    assert_merged(resolver, "e1", "e2")


def test_unprotected_props_do_not_block_merge(store: Store, resolver: Resolver):
    entity1 = e(
        store.dataset,
        "Directorship",
        "e1",
        properties={
            "director": "a",
            "organization": "b",
            "role": "director",
            "sourceUrl": "https://example.com/1",
        },
    )
    entity2 = e(
        store.dataset,
        "Directorship",
        "e2",
        properties={
            "director": "a",
            "organization": "b",
            "role": "director",
            "sourceUrl": "https://example.com/2",
        },
    )
    add_entities(store, [entity1, entity2])

    dedupe_edges(resolver, store.default_view())
    assert_merged(resolver, "e1", "e2")


def test_occupancy_period_regression(store: Store, resolver: Resolver):
    entities = [
        e(
            store.dataset,
            "Occupancy",
            f"e{idx}",
            properties={
                "holder": "person",
                "post": "position",
                "status": "ended",
                "periodStart": start,
                "periodEnd": end,
            },
        )
        for idx, (start, end) in enumerate(
            [
                ("2003", "2007"),
                ("2007", "2010"),
                ("2010", "2014"),
                ("2014", "2019"),
                ("2019", "2024"),
            ],
            start=1,
        )
    ]
    add_entities(store, entities)

    dedupe_edges(resolver, store.default_view())
    assert_not_merged(resolver, "e1", "e2", "e3", "e4", "e5")
