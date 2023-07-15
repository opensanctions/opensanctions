from zavod.meta import get_catalog
from zavod.entity import Entity

TEST_DATASET = {
    "name": "test",
    "title": "Test Dataset",
    "lookups": {
        "type.country": {
            "lowercase": True,
            "options": [
                {"match": "MOORICA", "value": "us"},
            ],
        }
    },
}


def test_basic():
    catalog = get_catalog()
    test_ds = catalog.make_dataset(TEST_DATASET)
    entity = Entity(test_ds, {"schema": "Person"})
    assert len(list(entity.statements)) == 0
    entity.id = "test_entity"
    assert len(list(entity.statements)) == 1
    entity.add("name", "John Doe")
    assert len(list(entity.statements)) == 2
    entity.add("nationality", {"Britain"})
    assert len(list(entity.statements)) == 3
    entity.add("nationality", {"moorica"})
    assert len(list(entity.statements)) == 4
    assert "us" in entity.get("nationality")
