"""Tests for emitting FollowTheMoney entities from funes extraction hits."""

from typing import Any

import pytest

from zavod.context import Context
from zavod.entity import Entity
from zavod.meta import Dataset
from zavod.shed.funes.emit import emit_hit
from zavod.shed.funes.extract import Hit, Person, Position
from zavod.tests.util import get_test_view, make_context

SOURCE_URL = "https://example.com/justices"


def make_dataset() -> Dataset:
    funes: dict[str, Any] = {
        "people_sought": "judges",
        "subject_label": "Court",
        "subjects": [{"name": "Example Court", "urls": [SOURCE_URL]}],
    }
    return Dataset(
        {
            "name": "test_funes",
            "title": "Test",
            "prefix": "tst",
            "config": {"funes": funes},
        }
    )


def make_hit(
    *,
    organization: str | None = "Example Court",
    start_date: str | None = "2022-01-01",
    end_date: str | None = None,
    name: str = "Alice Example",
    dob: str | None = "1980-01-02",
) -> Hit:
    return Hit(
        persons=[
            Person(
                name=name,
                dob=dob,
                bio="A lawyer.",
                countries=["Germany"],
                positions=[
                    Position(
                        name="Justice",
                        organization=organization,
                        description="Decides cases.",
                        jurisdiction="Germany",
                        start_date=start_date,
                        end_date=end_date,
                    )
                ],
            )
        ]
    )


def emit(dataset: Dataset, hit: Hit) -> Context:
    context = make_context(dataset)
    context.begin()
    emit_hit(context, hit, source_url=SOURCE_URL)
    return context


def read_entities(dataset: Dataset, context: Context) -> list[Entity]:
    context.finalize_statements()
    context.close()
    view = get_test_view(dataset, clear=True)
    try:
        return list(view.entities())
    finally:
        view.store.close()


def by_schema(entities: list[Entity]) -> dict[str, list[Entity]]:
    result: dict[str, list[Entity]] = {}
    for entity in entities:
        result.setdefault(entity.schema.name, []).append(entity)
    return result


def test_emits_full_graph(zavod_db: None) -> None:
    dataset = make_dataset()
    context = emit(dataset, make_hit())
    person_id = context.make_id("Alice Example", "1980-01-02")
    organization_id = context.make_id("Example Court")

    entities = by_schema(read_entities(dataset, context))
    assert set(entities) == {"Person", "Position", "Organization", "Occupancy"}

    person = entities["Person"][0]
    assert person.id == person_id
    assert person.get("name") == ["Alice Example"]
    assert person.get("citizenship") == ["de"]
    assert person.get("birthDate") == ["1980-01-02"]
    assert person.get("notes") == ["A lawyer."]

    organization = entities["Organization"][0]
    assert organization.id == organization_id
    assert organization.get("name") == ["Example Court"]

    position = entities["Position"][0]
    assert position.get("name") == ["Justice"]
    assert position.get("country") == ["de"]
    assert position.get("description") == ["Decides cases."]
    assert position.get("organization") == [organization_id]

    occupancy = entities["Occupancy"][0]
    assert occupancy.get("holder") == [person_id]
    assert occupancy.get("post") == [position.id]
    assert occupancy.get("startDate") == ["2022-01-01"]
    # emit passes no_end_implies_current=False, so an undated occupancy is
    # unknown and carries no status.
    assert occupancy.get("status") == []


def test_organization_absent(zavod_db: None) -> None:
    dataset = make_dataset()
    context = emit(dataset, make_hit(organization=None))

    entities = by_schema(read_entities(dataset, context))
    assert "Organization" not in entities
    assert entities["Position"][0].get("organization") == []


def test_after_office_position_is_dropped(zavod_db: None) -> None:
    """A position whose occupancy is rejected drops the whole graph, not
    just the occupancy."""
    dataset = make_dataset()
    context = emit(dataset, make_hit(start_date="1980-01-01", end_date="1990-01-01"))

    assert read_entities(dataset, context) == []


def test_missing_definition_raises() -> None:
    dataset = Dataset({"name": "no_funes", "title": "Test", "prefix": "nf"})
    context = make_context(dataset)
    context.begin()
    try:
        with pytest.raises(ValueError, match="no_funes"):
            emit_hit(context, make_hit(), source_url=SOURCE_URL)
    finally:
        context.close()
