from zavod.entity import Entity
from zavod.meta import Dataset
from zavod.exporters.consolidate import _redundant_dates


def simplify_dates(entity: Entity) -> Entity:
    return entity.clone_without_statements(_redundant_dates(entity))


ENTITY = {
    "id": "demo",
    "schema": "Person",
    "properties": {
        "birthDate": ["1972", "1972-04", "1972-04-12"],
        "createdAt": ["2023-01-01", "2023-03-03"],
    },
}


DATASET = Dataset({"name": "test_consolidate"})


def testsimplify_dates():
    entity = Entity.from_data(DATASET, ENTITY)
    assert len(entity.get("birthDate")) == 3
    assert len(entity.get("createdAt")) == 2
    simple = simplify_dates(entity)
    assert simple.get("birthDate") == ["1972-04-12"]
    assert simple.get("createdAt") == ["2023-01-01"]
