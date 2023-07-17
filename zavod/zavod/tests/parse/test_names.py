from followthemoney import model
from nomenklatura.entity import CompositeEntity
from zavod.parse import make_name, apply_name


ENTITY = {
    "id": "bla",
    "schema": "Person",
}


def test_make_name():
    name = make_name(first_name="John", last_name="Doe")
    assert name == "John Doe"


def test_entity_name():
    entity = CompositeEntity.from_dict(model, ENTITY)
    apply_name(
        entity,
        first_name="John",
        second_name="Brandon",
        last_name="Doe",
        lang="eng",
    )
    assert entity.caption == "John Brandon Doe"
    for stmt in entity.get_statements("name"):
        assert stmt.lang == "eng"
