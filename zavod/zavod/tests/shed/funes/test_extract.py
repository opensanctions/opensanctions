"""Tests for the extraction agent's own logic: the view_resource tool and
the wiring of the real agent (tool registration, brief in instructions)."""

import asyncio
import json

import pytest
from pydantic_ai import BinaryContent, RunContext, ToolFailed
from pydantic_ai.messages import (
    ModelResponse,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
)
from pydantic_ai.models.function import FunctionModel
from pydantic_ai.usage import RunUsage

from zavod.shed.funes.agents import Brief
from zavod.shed.funes.extract import (
    ExtractionDependencies,
    Miss,
    extraction_agent,
    view_resource,
)


def _deps(media_types, data=b"fake-image-bytes"):
    async def read_resource(body_path: str) -> bytes:
        return data

    return ExtractionDependencies(
        brief=Brief(
            people_sought="Board members",
            subject_label="Organization",
            subject="Example Foundation",
        ),
        read_resource=read_resource,
        resource_media_types=media_types,
    )


def _run_context(deps: ExtractionDependencies) -> RunContext:
    """A bare context; view_resource only reads ``deps`` from it."""
    return RunContext(deps=deps, model=None, usage=RunUsage())


# --- view_resource ---


def test_view_resource_returns_captured_image() -> None:
    deps = _deps({"bodies/a.jpg": "image/jpeg"}, b"\xff\xd8jpeg")
    content = asyncio.run(view_resource(_run_context(deps), "bodies/a.jpg"))
    assert content == BinaryContent(b"\xff\xd8jpeg", media_type="image/jpeg")


def test_view_resource_fails_on_unknown_body_path() -> None:
    deps = _deps({"bodies/a.jpg": "image/jpeg"})
    with pytest.raises(ToolFailed, match="no captured resource"):
        asyncio.run(view_resource(_run_context(deps), "made/up"))


def test_view_resource_fails_on_unviewable_media_type() -> None:
    deps = _deps({"bodies/v.svg": "image/svg+xml"})
    with pytest.raises(ToolFailed, match="cannot be viewed"):
        asyncio.run(view_resource(_run_context(deps), "bodies/v.svg"))


# --- the real agent's wiring ---


def test_agent_exposes_tool_and_brief() -> None:
    """The constructed agent registers view_resource for the model and
    renders the run's brief into its instructions. Both losses would
    degrade real runs silently, so they are pinned here explicitly."""
    seen: dict[str, str] = {}

    def fn(messages, info):
        seen["instructions"] = info.instructions
        if not any(
            getattr(p, "tool_name", None) == "view_resource"
            for m in messages
            for p in getattr(m, "parts", [])
        ):
            return ModelResponse(
                parts=[
                    ToolCallPart(
                        tool_name="view_resource", args={"body_path": "bodies/a.jpg"}
                    )
                ]
            )
        # NativeOutput union envelope: {"result": {"kind": ..., "data": ...}}
        return ModelResponse(
            parts=[
                TextPart(
                    json.dumps(
                        {
                            "result": {
                                "kind": "Miss",
                                "data": {"reason": "No officeholders on the page."},
                            }
                        }
                    )
                )
            ]
        )

    deps = _deps({"bodies/a.jpg": "image/jpeg"}, b"\xff\xd8jpeg")
    with extraction_agent.override(model=FunctionModel(fn)):
        result = asyncio.run(extraction_agent.run("ignored prompt", deps=deps))

    assert (
        "People sought: Board members\nOrganization: Example Foundation"
        in seen["instructions"]
    )
    assert isinstance(result.output, Miss)
    assert result.output.reason == "No officeholders on the page."
    [returned] = [
        p
        for m in result.all_messages()
        for p in getattr(m, "parts", [])
        if isinstance(p, ToolReturnPart) and p.tool_name == "view_resource"
    ]
    assert returned.content == BinaryContent(b"\xff\xd8jpeg", media_type="image/jpeg")
