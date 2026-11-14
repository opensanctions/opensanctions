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


def test_future_birth_date_rejected():
    catalog = get_catalog()
    test_ds = catalog.make_dataset(TEST_DATASET)
    entity = Entity(test_ds, {"schema": "Person"})
    entity.id = "test_entity"

    entity.add("birthDate", "2999-01-01")
    assert entity.get("birthDate") == []

    entity.add("birthDate", "1988-07-16")
    assert "1988-07-16" in entity.get("birthDate")

    # The guard applies only to birth dates.
    entity.add("deathDate", "2999-01-01")
    assert "2999-01-01" in entity.get("deathDate")


def test_target_logic():
    catalog = get_catalog()
    test_ds = catalog.make_dataset(TEST_DATASET)
    entity = Entity(test_ds, {"schema": "LegalEntity"})
    entity.id = "test_entity"
    assert not entity.target, entity.to_dict()

    entity.add("topics", "sanction")
    assert entity.target, entity.to_dict()


def test_clone_without_statements():
    catalog = get_catalog()
    test_ds = catalog.make_dataset(TEST_DATASET)
    entity = Entity(test_ds, {"schema": "Person", "id": "jane"})
    entity.add("name", ["Jane Doe", "JANE DOE"])
    entity.add("nationality", "de")
    entity.extra_referents.add("other-jane")
    entity.last_change = "2024-01-01T00:00:00"
    upper = {s for s in entity._statements["name"] if s.value == "JANE DOE"}

    cloned = entity.clone_without_statements({"name": upper})

    assert cloned is not entity
    assert cloned.id == "jane"
    assert cloned.schema == entity.schema
    assert cloned.dataset == entity.dataset
    assert cloned.last_change == "2024-01-01T00:00:00"
    assert cloned.referents == {"other-jane"}
    assert cloned.get("name") == ["Jane Doe"]
    # The original keeps its statements.
    assert sorted(entity.get("name")) == ["JANE DOE", "Jane Doe"]
    # The copy has its own sets, holding the same statement objects.
    assert cloned._statements["nationality"] == entity._statements["nationality"]
    assert cloned._statements["nationality"] is not entity._statements["nationality"]
    cloned_name = next(iter(cloned._statements["name"]))
    assert any(s is cloned_name for s in entity._statements["name"])
    # The referents are the copy's own.
    cloned.extra_referents.add("third-jane")
    assert "third-jane" not in entity.extra_referents
