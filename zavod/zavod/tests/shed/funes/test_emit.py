"""Tests for emitting FollowTheMoney entities from funes extraction hits."""

from datetime import timedelta
from typing import Any

import pytest
from sqlalchemy import select

from zavod import settings
from zavod.context import Context
from zavod.entity import Entity
from zavod.meta import Dataset
from zavod.shed.funes.emit import emit_hit
from zavod.shed.funes.extract import Hit, Person, Position
from zavod.stateful.model import position_table
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
            "dates": {
                "formats": ["%d. %b %Y"],
                "months": {"mar": "März"},
            },
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
    hit = make_hit()
    context = emit(dataset, hit)
    person_id = context.make_id("Alice Example", "1980-01-02")
    position_id = context.make_id("Justice", "Germany")
    organization_id = context.make_id("Example Court")

    entities = by_schema(read_entities(dataset, context))
    assert set(entities) == {"Person", "Position", "Organization", "Occupancy"}

    person = entities["Person"][0]
    assert person.id == person_id
    assert person.get("name") == ["Alice Example"]
    assert person.get("citizenship") == ["de"]
    assert person.get("birthDate") == ["1980-01-02"]
    assert person.get("notes") == ["A lawyer."]
    assert "role.pep" in person.get("topics")

    organization = entities["Organization"][0]
    assert organization.id == organization_id
    assert organization.get("name") == ["Example Court"]

    position = entities["Position"][0]
    assert position.id == position_id
    assert position.get("name") == ["Justice"]
    assert position.get("country") == ["de"]
    assert position.get("organization") == [organization_id]
    assert position.get("description") == ["Decides cases."]

    occupancy = entities["Occupancy"][0]
    assert occupancy.get("holder") == [person_id]
    assert occupancy.get("post") == [position_id]
    assert occupancy.get("startDate") == ["2022-01-01"]
    assert occupancy.get("status") == []


def test_organization_absent(zavod_db: None) -> None:
    dataset = make_dataset()
    context = emit(dataset, make_hit(organization=None))

    entities = by_schema(read_entities(dataset, context))
    assert "Organization" not in entities
    assert entities["Position"][0].get("organization") == []


def test_status_without_end_date_is_unknown(zavod_db: None) -> None:
    dataset = make_dataset()
    context = emit(dataset, make_hit(start_date="2022-01-01"))

    occupancy = by_schema(read_entities(dataset, context))["Occupancy"][0]
    # make_occupancy leaves status unset for UNKNOWN.
    assert occupancy.get("status") == []


def test_recent_end_date_is_ended(zavod_db: None) -> None:
    dataset = make_dataset()
    end_date = (settings.RUN_TIME - timedelta(days=30)).date().isoformat()
    context = emit(dataset, make_hit(start_date="2020-01-01", end_date=end_date))

    occupancy = by_schema(read_entities(dataset, context))["Occupancy"][0]
    assert occupancy.get("status") == ["ended"]


def test_after_office_position_is_dropped(zavod_db: None) -> None:
    dataset = make_dataset()
    context = emit(dataset, make_hit(start_date="1980-01-01", end_date="1990-01-01"))

    assert read_entities(dataset, context) == []


def test_position_is_enrolled(zavod_db: None) -> None:
    dataset = make_dataset()
    context = emit(dataset, make_hit())
    position_id = context.make_id("Justice", "Germany")

    statement = select(position_table).where(position_table.c.entity_id == position_id)
    rows = context.db.execute(statement).fetchall()
    context.close()
    assert len(rows) == 1
    assert rows[0].is_pep is True


def test_dates_are_cleaned(zavod_db: None) -> None:
    dataset = make_dataset()
    context = emit(dataset, make_hit(start_date="14. März 2021"))

    occupancy = by_schema(read_entities(dataset, context))["Occupancy"][0]
    assert occupancy.get("startDate") == ["2021-03-14"]


def test_missing_definition_raises() -> None:
    dataset = Dataset({"name": "no_funes", "title": "Test", "prefix": "nf"})
    context = make_context(dataset)
    context.begin()
    try:
        with pytest.raises(ValueError, match="no_funes"):
            emit_hit(context, make_hit(), source_url=SOURCE_URL)
    finally:
        context.close()
