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


def test_add_cast_keeps_provenance():
    catalog = get_catalog()
    test_ds = catalog.make_dataset(TEST_DATASET)

    # The entity is widened from LegalEntity to Person, so the cast happens.
    cast = Entity(test_ds, {"schema": "LegalEntity"})
    cast.id = "cast_entity"
    cast.add_cast(
        "Person",
        "position",
        "Minister",
        lang="deu",
        original_value="Minister (a. D.)",
        origin="test",
    )
    (cast_stmt,) = cast.get_statements("position")
    assert cast_stmt.lang == "deu", cast_stmt
    assert cast_stmt.original_value == "Minister (a. D.)", cast_stmt
    assert cast_stmt.origin == "test", cast_stmt

    # Person already has the property, so no cast is needed. The provenance
    # must not depend on the schema the entity happened to have.
    direct = Entity(test_ds, {"schema": "Person"})
    direct.id = "direct_entity"
    direct.add_cast(
        "Person",
        "position",
        "Minister",
        lang="deu",
        original_value="Minister (a. D.)",
        origin="test",
    )
    (direct_stmt,) = direct.get_statements("position")
    assert direct_stmt.lang == "deu", direct_stmt
    assert direct_stmt.original_value == "Minister (a. D.)", direct_stmt
    assert direct_stmt.origin == "test", direct_stmt


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
