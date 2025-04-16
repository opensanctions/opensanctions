from typing import Dict, Optional

import pytest
from nomenklatura import Resolver, Store
from nomenklatura.store import SimpleMemoryStore

from zavod.entity import Entity
from zavod.integration.edges import dedupe_edges, get_vertices, make_key
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
    properties: Dict[str, str] = {},
) -> Entity:
    properties["startDate"] = start
    properties["endDate"] = end
    return Entity.from_data(
        dataset,
        {
            "schema": schema,
            "id": id,
            "properties": {p: [v] for p, v in properties.items()},
        },
    )


def test_dedupe_edges_same_temporal_extent(store: Store, resolver: Resolver):
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
    view = store.default_view()

    dedupe_edges(resolver, view)
    # Same basic grouping key results in merge, description has no impact
    assert resolver.get_canonical("e1") == resolver.get_canonical("e2")


def test_dedupe_edges_different_extra_prop(store: Store, resolver: Resolver):
    # with temporal extent
    entity1 = e(
        store.dataset,
        "UnknownLink",
        "e1",
        "2020-01-01",
        "2021-01-01",
        {"subject": "a", "object": "b", "role": "AAA"},
    )
    entity2 = e(
        store.dataset,
        "UnknownLink",
        "e2",
        "2020-01-01",
        "2021-01-01",
        {
            "subject": "a",
            "object": "b",
            "role": "BBB",
        },
    )
    add_entities(store, [entity1, entity2])
    view = store.default_view()

    dedupe_edges(resolver, view)
    # Different extra prop for schema prevents merge
    assert resolver.get_canonical("e1") != resolver.get_canonical("e2")


def test_no_dedupe_for_different_schemas(store: Store, resolver: Resolver):
    # different schemas
    entity1 = e(store.dataset, "Ownership", "e1", {"owner": "a", "asset": "b"})
    entity2 = e(
        store.dataset, "Directorship", "e2", {"director": "a", "organization": "b"}
    )
    add_entities(store, [entity1, entity2])
    view = store.default_view()

    dedupe_edges(resolver, view)
    assert resolver.get_canonical("e1") != resolver.get_canonical("e2")


def test_group_common_start(store: Store, resolver: Resolver):
    # common start
    entity1 = e(
        store.dataset,
        "Ownership",
        "e1",
        "2020-01-01",
        None,
        {"owner": "a", "asset": "b"},
    )
    entity2 = e(
        store.dataset,
        "Ownership",
        "e2",
        "2020-01-01",
        "2021-01-01",
        {
            "owner": "a",
            "asset": "b",
        },
    )
    add_entities(store, [entity1, entity2])
    view = store.default_view()

    dedupe_edges(resolver, view)
    assert resolver.get_canonical("e1") == resolver.get_canonical("e2")


def test_group_common_start_multiple_options(store: Store, resolver: Resolver):
    # common start
    entity1 = e(
        store.dataset,
        "Ownership",
        "e1",
        "2020-01-01",
        None,
        {"owner": "a", "asset": "b"},
    )
    entity2 = e(
        store.dataset,
        "Ownership",
        "e2",
        "2020-01-01",
        "2021-01-01",
        {
            "owner": "a",
            "asset": "b",
        },
    )
    entity3 = e(
        store.dataset,
        "Ownership",
        "e3",
        "2020-01-01",
        "2020-01-02",
        {"owner": "a", "asset": "b"},
    )

    add_entities(store, [entity1, entity2, entity3])
    view = store.default_view()

    dedupe_edges(resolver, view)
    assert resolver.get_canonical("e1") != resolver.get_canonical("e2")
    assert resolver.get_canonical("e1") != resolver.get_canonical("e3")
    assert resolver.get_canonical("e2") != resolver.get_canonical("e3")


def test_make_key(testdataset1: Dataset):
    # End date is defined
    entity2 = e(
        testdataset1,
        "Ownership",
        "e2",
        "2020-01-01",
        "2021-01-01",
        {
            "owner": "a",
            "asset": "b",
            "role": "FOOBAR",
        },
    )
    vertices = get_vertices(entity2)
    key = make_key(vertices, entity2, {})
    # only basics used for key
    assert key.source == "a"
    assert key.target == "b"
    assert key.schema.name == "Ownership"
    assert key.temporal_start[1] == "2020-01-01"
    assert key.temporal_end[1] == "2021-01-01"
    assert key.role is None

    key = make_key(vertices, entity2, {}, blank_end=True)
    # end date gets blanked
    assert key.temporal_start[1] == "2020-01-01"
    assert key.temporal_end is None

    key = make_key(vertices, entity2, {"Ownership": ["role"]})
    # extra prop used for key
    assert key.role == ("FOOBAR",)
