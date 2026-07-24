from zavod.entity import Entity
from zavod.meta import Dataset
from zavod.runner.util import is_analyzer_stub

GRAPH = Dataset({"name": "ann_graph_topics", "title": "Graph"})
SOURCE = Dataset({"name": "src", "title": "Source"})


def _entity(dataset: Dataset, properties: dict[str, list[str]]) -> Entity:
    return Entity.from_data(
        dataset,
        {"schema": "Person", "id": "e", "properties": properties},
    )


def test_analyzer_only_entity_is_stub() -> None:
    assert is_analyzer_stub(_entity(GRAPH, {"topics": ["role.rca"]}))


def test_source_entity_is_not_stub() -> None:
    assert not is_analyzer_stub(_entity(SOURCE, {"name": ["John Doe"]}))


def test_mixed_entity_is_not_stub() -> None:
    # An analyzer patch merged with source data has something to match on.
    entity = _entity(GRAPH, {"topics": ["role.rca"]})
    prop = entity.schema.get("name")
    assert prop is not None
    entity.unsafe_add(prop, "John Doe", dataset="src")
    assert not is_analyzer_stub(entity)
