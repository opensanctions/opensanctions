"""Tests for the funes inspection runner.

The capture seam is a real async function returning Pravda ``Snapshot``
values whose artifacts live in a dict; the extraction agent is the real
agent driven by a FunctionModel. No browser and no model provider are
involved.
"""

import asyncio
import json
import uuid
from datetime import UTC, datetime
from typing import Any

from pravda import Snapshot
from pydantic_ai.messages import ModelResponse, TextPart
from pydantic_ai.models.function import FunctionModel
from sqlalchemy import select

from zavod.context import Context
from zavod.meta import Dataset
from zavod.shed.funes.agents import Brief
from zavod.shed.funes.catalogue import sync_catalogue
from zavod.shed.funes.definitions import dataset_definition
from zavod.shed.funes.extract import extraction_agent
from zavod.shed.funes.model import (
    InspectionOutcome,
    SnapshotStatus,
    attempt_table,
    candidate_table,
    snapshot_table,
)
from zavod.shed.funes.runner import _inspect_candidate
from zavod.tests.util import get_test_view, make_context

SOURCE_URL = "https://example.com/justices"

BRIEF = Brief(
    people_sought="judges",
    subject_label="Court",
    subject="Example Court",
)

MODEL = "test-model"
THINKING = "low"


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


def make_snapshot(
    html: str | None = "<html><body>Justices of the court</body></html>",
) -> Snapshot:
    """A snapshot whose rendered HTML artifact lives in ARTIFACTS."""
    usable = html is not None
    return Snapshot(
        id=uuid.uuid4(),
        url=SOURCE_URL,
        final_url=SOURCE_URL if usable else None,
        captured_at=datetime.now(UTC),
        http_status=200 if usable else None,
        error=None if usable else "TimeoutError: 30000ms exceeded\n  at goto",
        rendered_html="/storage/example.com/rendered.html" if usable else None,
        screenshot=None,
        http_archive=None,
    )


ARTIFACTS = {"/storage/example.com/rendered.html": b"<html>Justices</html>"}


async def read_resource(path: str) -> bytes:
    return ARTIFACTS[path]


def native_output(member: str, payload: dict[str, Any]) -> str:
    """Serialize one PageResult member in the native-output envelope."""
    return json.dumps({"result": {"kind": member, "data": payload}})


def output_model(member: str, payload: dict[str, Any]) -> FunctionModel:
    def fn(messages: Any, info: Any) -> ModelResponse:
        return ModelResponse(parts=[TextPart(native_output(member, payload))])

    return FunctionModel(fn)


def candidate_id(context: Context) -> uuid.UUID:
    return context.db.execute(
        select(candidate_table.c.id).where(candidate_table.c.url == SOURCE_URL)
    ).scalar_one()


def read_attempt(context: Context) -> Any:
    return context.db.execute(select(attempt_table)).one()


def inspect(
    context: Context,
    snapshot: Snapshot,
    model: FunctionModel,
) -> None:
    """Run one candidate inspection with the given captured snapshot."""

    async def capture(url: str) -> Snapshot:
        return snapshot

    with extraction_agent.override(model=model):
        asyncio.run(
            _inspect_candidate(
                context,
                capture=capture,
                read_resource=read_resource,
                model=MODEL,
                thinking=THINKING,
                brief=BRIEF,
                candidate_id=candidate_id(context),
                url=SOURCE_URL,
            )
        )


def unreachable_model() -> FunctionModel:
    """A model whose use fails the test: nothing may reach it."""

    def fn(messages: Any, info: Any) -> ModelResponse:
        raise AssertionError("the model must not be reached")

    return FunctionModel(fn)


def test_inspect_candidate_broken_capture(zavod_db: None, funes_db: None) -> None:
    dataset = make_dataset()
    context = make_context(dataset)
    context.begin()
    sync_catalogue(context, dataset_definition(dataset))
    inspect(context, make_snapshot(html=None), model=unreachable_model())

    attempt = read_attempt(context)
    assert attempt.status == SnapshotStatus.BROKEN
    assert attempt.outcome is None
    assert attempt.reason == (
        "missing artifact: rendered_html (TimeoutError: 30000ms exceeded)"
    )
    assert attempt.model is None

    context.finalize_statements()
    context.close()
    view = get_test_view(dataset, clear=True)
    try:
        assert list(view.entities()) == []
    finally:
        view.store.close()


def test_inspect_candidate_model_broken(zavod_db: None, funes_db: None) -> None:
    dataset = make_dataset()
    context = make_context(dataset)
    context.begin()
    sync_catalogue(context, dataset_definition(dataset))
    inspect(
        context,
        make_snapshot(),
        model=output_model(
            "BrokenSnapshot",
            {"reason": "A click-through consent gate blocks the page content."},
        ),
    )

    attempt = read_attempt(context)
    assert attempt.status == SnapshotStatus.BROKEN
    assert attempt.reason == "A click-through consent gate blocks the page content."
    assert attempt.model == MODEL

    context.finalize_statements()
    context.close()


def test_inspect_candidate_hit(zavod_db: None, funes_db: None) -> None:
    dataset = make_dataset()
    context = make_context(dataset)
    context.begin()
    sync_catalogue(context, dataset_definition(dataset))
    hit = {
        "persons": [
            {
                "name": "Alice Example",
                "dob": "1980-01-02",
                "bio": "A lawyer.",
                "countries": ["Germany"],
                "positions": [
                    {
                        "name": "Justice",
                        "organization": "Example Court",
                        "jurisdiction": "Germany",
                        "start_date": "2022-01-01",
                    }
                ],
            }
        ]
    }
    snapshot = make_snapshot()
    inspect(context, snapshot, model=output_model("Hit", hit))

    attempt = read_attempt(context)
    assert attempt.status == SnapshotStatus.USABLE
    assert attempt.outcome == InspectionOutcome.HIT
    assert attempt.reason is None
    assert attempt.model == MODEL

    snapshot_row = context.db.execute(select(snapshot_table)).one()
    assert snapshot_row.id == snapshot.id

    context.finalize_statements()
    context.close()
    view = get_test_view(dataset, clear=True)
    try:
        schemas = {entity.schema.name for entity in view.entities()}
        assert schemas == {"Person", "Position", "Organization", "Occupancy"}
    finally:
        view.store.close()


def test_inspect_candidate_miss(zavod_db: None, funes_db: None) -> None:
    dataset = make_dataset()
    context = make_context(dataset)
    context.begin()
    sync_catalogue(context, dataset_definition(dataset))
    inspect(
        context,
        make_snapshot(),
        model=output_model("Miss", {"reason": "The page lists no officeholders."}),
    )

    attempt = read_attempt(context)
    assert attempt.status == SnapshotStatus.USABLE
    assert attempt.outcome == InspectionOutcome.MISS
    assert attempt.reason == "The page lists no officeholders."

    context.finalize_statements()
    context.close()
    view = get_test_view(dataset, clear=True)
    try:
        assert list(view.entities()) == []
    finally:
        view.store.close()
