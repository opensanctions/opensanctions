"""Vocabulary shared by the pipeline's LLM agents.

Every agent runs against a trusted per-dataset brief and emits strict
output schemas; both conventions live here so the extraction and discovery
agents cannot drift apart.
"""

from dataclasses import dataclass
from typing import Annotated

from pydantic import BaseModel, ConfigDict, StringConstraints


class StrictModel(BaseModel):
    """Base for agent output schemas: unknown fields are a bug, not data."""

    model_config = ConfigDict(extra="forbid")


NonBlank = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


@dataclass(frozen=True)
class Brief:
    """Trusted per-dataset selection brief, shared by all agents."""

    people_sought: str
    subject_label: str
    subject: str


def render_brief(brief: Brief) -> str:
    """Render the trusted brief as dynamic instructions for any agent."""
    return (
        f"People sought: {brief.people_sought}\n{brief.subject_label}: {brief.subject}"
    )
