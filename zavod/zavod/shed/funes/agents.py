"""Shared definitions for the funes LLM agents."""

from dataclasses import dataclass
from typing import Annotated

from pydantic import BaseModel, ConfigDict, StringConstraints


class StrictModel(BaseModel):
    """Base for agent output schemas."""

    model_config = ConfigDict(extra="forbid")


NonBlank = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


@dataclass(frozen=True)
class Brief:
    """Selection criteria for one inspection: the people sought and the subject."""

    people_sought: str
    subject_label: str
    subject: str


def render_brief(brief: Brief) -> str:
    """Render the brief as dynamic instructions for an agent run."""
    return (
        f"People sought: {brief.people_sought}\n{brief.subject_label}: {brief.subject}"
    )
