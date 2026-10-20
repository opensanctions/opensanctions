"""Tests for the extraction schema, tool, and result union."""

import asyncio
import json

import pytest
from pydantic import ValidationError
from pydantic_ai import Agent, BinaryContent
from pydantic_ai.messages import (
    ModelResponse,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
)
from pydantic_ai.models.function import FunctionModel
from pydantic_ai.models.test import TestModel

from zavod.shed.funes.agents import Brief, render_brief
from zavod.shed.funes.extract import (
    BrokenSnapshot,
    ExtractionDependencies,
    Hit,
    Miss,
    Person,
    Position,
    extraction_agent,
    view_resource,
)

_PERSON = {
    "name": "Amina Diallo",
    "countries": ["Senegal"],
    "positions": [
        {
            "name": "Director",
            "organization": "Example Foundation",
            "start_date": "2021",
        },
        {"name": "Chair", "organization": "Example Foundation"},
    ],
}


def _native_text(member: str, payload: dict) -> str:
    """Serialize one PageResult member in the native-output union envelope.

    Multi-type native output validates one combined schema: an outer
    ``result`` object whose ``kind`` names the member class and whose ``data``
    carries the member payload.
    """
    return json.dumps({"result": {"kind": member, "data": payload}})


# --- union discrimination and validation ---


def test_hit_requires_at_least_one_person() -> None:
    hit = Hit(persons=[{"name": "A", "positions": [{"name": "Director"}]}])
    assert [p.name for p in hit.persons] == ["A"]
    assert [pos.name for pos in hit.persons[0].positions] == ["Director"]
    with pytest.raises(ValidationError):
        Hit(persons=[])
    with pytest.raises(ValidationError):
        Hit()


def test_output_models_reject_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        Miss(reason="no holders", unexpected=True)


# --- view_resource tool ---


def _view_agent(function):
    """A lightweight agent exposing view_resource, driven by *function*.

    The first model turn returns whatever *function* decides; a final text
    response ends the run so we can inspect the tool traffic.
    """
    return Agent(
        FunctionModel(function),
        output_type=str,
        tools=[view_resource],
        deps_type=ExtractionDependencies,
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


def _tool_return_part(result):
    parts = [p for m in result.all_messages() for p in getattr(m, "parts", [])]
    return parts


def test_view_resource_returns_binary_content() -> None:
    def fn(messages, info):
        assert [t.name for t in info.function_tools] == ["view_resource"]
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
        return ModelResponse(parts=[TextPart("done")])

    deps = _deps({"bodies/a.jpg": "image/jpeg"}, b"\xff\xd8jpeg")
    result = asyncio.run(_view_agent(fn).run("look", deps=deps))
    assert result.output == "done"
    returns = [
        p
        for p in _tool_return_part(result)
        if isinstance(p, ToolReturnPart) and p.tool_name == "view_resource"
    ]
    assert len(returns) == 1
    content = returns[0].content
    assert isinstance(content, BinaryContent)
    assert content.data == b"\xff\xd8jpeg"
    assert content.media_type == "image/jpeg"


def test_view_resource_fails_on_unknown_body_path() -> None:
    def fn(messages, info):
        if not any(
            isinstance(p, ToolReturnPart) and p.outcome == "failed"
            for m in messages
            for p in getattr(m, "parts", [])
        ):
            return ModelResponse(
                parts=[
                    ToolCallPart(
                        tool_name="view_resource", args={"body_path": "made/up"}
                    )
                ]
            )
        return ModelResponse(parts=[TextPart("done")])

    deps = _deps({"bodies/a.jpg": "image/jpeg"})
    result = asyncio.run(_view_agent(fn).run("look", deps=deps))
    failures = [
        p
        for p in _tool_return_part(result)
        if isinstance(p, ToolReturnPart) and p.outcome == "failed"
    ]
    assert len(failures) == 1
    assert "no captured resource" in failures[0].content


def test_view_resource_fails_on_unsupported_media_type() -> None:
    def fn(messages, info):
        if not any(
            isinstance(p, ToolReturnPart) and p.outcome == "failed"
            for m in messages
            for p in getattr(m, "parts", [])
        ):
            return ModelResponse(
                parts=[
                    ToolCallPart(
                        tool_name="view_resource", args={"body_path": "bodies/v.svg"}
                    )
                ]
            )
        return ModelResponse(parts=[TextPart("done")])

    deps = _deps({"bodies/v.svg": "image/svg+xml"})
    result = asyncio.run(_view_agent(fn).run("look", deps=deps))
    failures = [
        p
        for p in _tool_return_part(result)
        if isinstance(p, ToolReturnPart) and p.outcome == "failed"
    ]
    assert len(failures) == 1
    assert "image/svg+xml" in failures[0].content


def test_view_resource_reads_through_dependencies() -> None:
    """The tool must fetch bytes via deps.read_resource, not by itself."""
    seen: list[str] = []

    def fn(messages, info):
        if not any(
            isinstance(p, ToolReturnPart)
            for m in messages
            for p in getattr(m, "parts", [])
        ):
            return ModelResponse(
                parts=[
                    ToolCallPart(
                        tool_name="view_resource", args={"body_path": "bodies/p.png"}
                    )
                ]
            )
        return ModelResponse(parts=[TextPart("done")])

    async def read_resource(body_path: str) -> bytes:
        seen.append(body_path)
        return b"pngdata"

    deps = ExtractionDependencies(
        brief=Brief(
            people_sought="Board members",
            subject_label="Organization",
            subject="Example Foundation",
        ),
        read_resource=read_resource,
        resource_media_types={"bodies/p.png": "image/png"},
    )
    result = asyncio.run(_view_agent(fn).run("look", deps=deps))
    assert seen == ["bodies/p.png"]
    returns = [
        p
        for p in _tool_return_part(result)
        if isinstance(p, ToolReturnPart) and p.tool_name == "view_resource"
    ]
    assert returns[0].content == BinaryContent(b"pngdata", media_type="image/png")


# --- full runs of the real extraction agent, its output union, and the
# view_resource tool wired exactly as the worker wires them ---


def test_agent_function_model_returns_hit_with_nested_graph() -> None:
    def fn(messages, info):
        return ModelResponse(
            parts=[TextPart(_native_text("Hit", {"persons": [_PERSON]}))]
        )

    with extraction_agent.override(model=FunctionModel(fn)):
        result = asyncio.run(extraction_agent.run("ignored prompt", deps=_deps({})))

    assert isinstance(result.output, Hit)
    [returned] = result.output.persons
    assert isinstance(returned, Person)
    assert returned.name == "Amina Diallo"
    assert returned.countries == ["Senegal"]
    assert [p.name for p in returned.positions] == ["Director", "Chair"]
    assert isinstance(returned.positions[0], Position)
    assert returned.positions[0].organization == "Example Foundation"
    assert returned.positions[0].start_date == "2021"


def test_agent_function_model_returns_miss() -> None:
    def fn(messages, info):
        return ModelResponse(
            parts=[
                TextPart(
                    _native_text("Miss", {"reason": "No officeholders; news article."})
                )
            ]
        )

    with extraction_agent.override(model=FunctionModel(fn)):
        result = asyncio.run(extraction_agent.run("ignored prompt", deps=_deps({})))

    assert isinstance(result.output, Miss)
    assert result.output.reason == "No officeholders; news article."


def test_agent_function_model_returns_broken_snapshot() -> None:
    def fn(messages, info):
        return ModelResponse(
            parts=[
                TextPart(
                    _native_text("BrokenSnapshot", {"reason": "Cloudflare challenge"})
                )
            ]
        )

    with extraction_agent.override(model=FunctionModel(fn)):
        result = asyncio.run(extraction_agent.run("ignored prompt", deps=_deps({})))

    assert isinstance(result.output, BrokenSnapshot)
    assert result.output.reason == "Cloudflare challenge"


def test_agent_test_model_views_resource_and_produces_valid_result() -> None:
    # TestModel generates 'a' for a string argument, so the media-type map
    # must carry that body path for the generated tool call to succeed. Its
    # default profile claims no native JSON-schema support and its generated
    # text would not satisfy the schema, so hand it a valid union payload
    # (miss: this stub does not model real page content) as the final text.
    deps = _deps({"a": "image/png"}, b"\x89PNG-fake")
    test_model = TestModel(
        profile={"supports_json_schema_output": True},
        custom_output_text=_native_text(
            "Miss", {"reason": "No officeholders on the page."}
        ),
    )
    with extraction_agent.override(model=test_model):
        result = asyncio.run(extraction_agent.run("ignored prompt", deps=deps))

    assert isinstance(result.output, Miss)
    assert result.output.reason == "No officeholders on the page."
    parts = _tool_return_part(result)
    [call] = [
        p
        for p in parts
        if isinstance(p, ToolCallPart) and p.tool_name == "view_resource"
    ]
    assert call.args == {"body_path": "a"}
    [image_return] = [
        p
        for p in parts
        if isinstance(p, ToolReturnPart) and p.tool_name == "view_resource"
    ]
    assert image_return.content == BinaryContent(
        b"\x89PNG-fake", media_type="image/png"
    )


# --- shared brief instructions ---


_BRIEF = Brief(
    people_sought="Board members",
    subject_label="Organization",
    subject="Example Foundation",
)


def test_extraction_agent_receives_the_brief_instructions() -> None:
    brief = "People sought: Board members\nOrganization: Example Foundation"
    assert render_brief(_BRIEF) == brief
    seen: dict[str, str] = {}

    def extraction_fn(messages, info):
        seen["extraction"] = info.instructions
        return ModelResponse(
            parts=[TextPart(_native_text("Miss", {"reason": "No board members here."}))]
        )

    with extraction_agent.override(model=FunctionModel(extraction_fn)):
        asyncio.run(extraction_agent.run("ignored prompt", deps=_deps({})))

    assert brief in seen["extraction"]
