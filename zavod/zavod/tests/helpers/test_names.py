from followthemoney import model
from nomenklatura.entity import CompositeEntity

from zavod.context import Context
from zavod.helpers import make_name, apply_name


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


def test_full_name():
    entity = CompositeEntity.from_dict(model, ENTITY)
    apply_name(
        entity,
        full="Zorro",
        first_name="John",
        second_name="Brandon",
        last_name="Doe",
        lang="eng",
    )
    assert entity.caption == "Zorro"
    for stmt in entity.get_statements("name"):
        assert stmt.lang == "eng"


def test_alias_name():
    entity = CompositeEntity.from_dict(model, ENTITY)
    apply_name(
        entity,
        first_name="John",
        second_name="Brandon",
        last_name="Doe",
        lang="eng",
        alias=True,
    )
    assert entity.get("name") == []
    assert "John Brandon Doe" in entity.get("alias")
    apply_name(
        entity,
        first_name="Johnny",
        last_name="Doe",
        lang="eng",
        is_weak=True,
    )
    assert "Johnny Doe" in entity.get("weakAlias")


def test_company_name(vcontext: Context):
    entity = vcontext.make("Company")
    entity.id = "bla"
    apply_name(
        entity,
        first_name="Hansen",
        last_name="Enterprises",
        lang="eng",
        alias=True,
        quiet=True,
    )
    assert entity.get("name") == []
    assert "Hansen Enterprises" in entity.get("alias")
    assert entity.get("firstName", quiet=True) == []
