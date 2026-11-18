from nomenklatura.resolver import Linker

from zavod.entity import Entity
from zavod.exporters.consolidate import consolidate_entity
from zavod.meta import Dataset

DATASET = Dataset({"name": "test_consolidate"})


def make_person(id: str, **properties: list[str]) -> Entity:
    return Entity.from_data(
        DATASET, {"schema": "Person", "id": id, "properties": properties}
    )


def test_consolidate_leaves_the_entity_unchanged() -> None:
    entity = make_person(
        "jane",
        name=["Jane Doe", "JANE DOE"],
        birthDate=["1972", "1972-04-12"],
        nationality=["de"],
    )
    statements = {p: set(s) for p, s in entity._statements.items()}

    consolidated = consolidate_entity(Linker({}), entity)

    assert consolidated is not entity
    assert consolidated.get("birthDate") == ["1972-04-12"]
    assert len(consolidated.get("name")) == 1
    # The original keeps every statement, as the sources stated them.
    assert entity._statements == statements
    assert sorted(entity.get("birthDate")) == ["1972", "1972-04-12"]
    assert sorted(entity.get("name")) == ["JANE DOE", "Jane Doe"]
    # Properties consolidation didn't touch keep all their statements.
    assert consolidated._statements["nationality"] == entity._statements["nationality"]


def test_consolidate_returns_an_entity_with_nothing_to_simplify() -> None:
    entity = make_person("jane", name=["Jane Doe"], birthDate=["1972-04-12"])
    assert consolidate_entity(Linker({}), entity) is entity
