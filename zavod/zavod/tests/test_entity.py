import pytest
from followthemoney.exc import InvalidData, InvalidModel

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


def test_extra_functions():
    catalog = get_catalog()
    test_ds = catalog.make_dataset(TEST_DATASET)
    entity = Entity(test_ds, {"schema": "LegalEntity"})
    entity.id = "test_entity"

    entity.add_cast("Person", "birthDate", None)
    assert entity.schema.name == "LegalEntity"
    entity.add_cast("Person", "birthDate", "1988")
    assert entity.schema.name == "Person"
    entity.add("phone", "123456789")
    assert entity.has("phone")

    with pytest.raises(InvalidData):
        prop = entity.schema.get("identification")
        entity.unsafe_add(prop, "123456789")

    with pytest.raises(InvalidData):
        entity.add_schema("Company")

    with pytest.raises(InvalidData):
        entity.add_cast("Company", "voenCode", "72379879")

    with pytest.raises(InvalidModel):
        entity.add_cast("Banana", "peel", "Acme Inc")

    assert entity.to_dict()["properties"]["birthDate"][0] == "1988"
