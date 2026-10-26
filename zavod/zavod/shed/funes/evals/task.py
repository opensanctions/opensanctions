"""Eval tasks that run the real agents on frozen captures."""

import json
from pathlib import Path

from pydantic_ai import ModelSettings
from pydantic_ai.settings import ThinkingEffort

from zavod.shed.funes.evals.models import FixtureInput
from zavod.shed.funes.agents import Brief
from zavod.shed.funes.extract import (
    ExtractionDependencies,
    PageResult,
    extraction_agent,
)
from zavod.shed.funes.outline import build_outline, har_resource_media_types
from zavod.shed.funes.page import build_prompt, metadata_from_html

FIXTURES = Path(__file__).parent / "fixtures"


async def extract(
    inputs: FixtureInput, model: str, thinking: ThinkingEffort
) -> PageResult:
    """Run metadata/outline/prompt construction and the extraction agent."""
    html = (FIXTURES / f"{inputs.fixture}.html").read_text(encoding="utf-8")
    har_path = FIXTURES / f"{inputs.fixture}.har.json"
    har = (
        json.loads(har_path.read_text(encoding="utf-8")) if har_path.exists() else None
    )
    final_url = inputs.final_url or inputs.url
    metadata = metadata_from_html(
        inputs.url,
        html,
        final_url=final_url,
        http_status=inputs.http_status,
        capture_error=inputs.capture_error,
    )
    outline = build_outline(final_url, html, har)

    async def read_resource(body_path: str) -> bytes:
        fixture_root = FIXTURES.resolve()
        resource_path = (FIXTURES / body_path).resolve()
        if not resource_path.is_relative_to(fixture_root):
            raise ValueError(
                f"resource body path escapes fixture directory: {body_path!r}"
            )
        return resource_path.read_bytes()

    deps = ExtractionDependencies(
        brief=Brief(
            people_sought=inputs.people_sought,
            subject_label=inputs.subject_label,
            subject=inputs.subject,
        ),
        read_resource=read_resource,
        resource_media_types=har_resource_media_types(har),
    )
    result = await extraction_agent.run(
        build_prompt(metadata, outline),
        model=model,
        model_settings=ModelSettings(thinking=thinking),
        deps=deps,
    )
    return result.output
