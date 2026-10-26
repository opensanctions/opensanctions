"""Case input types for the captured-page eval suites."""

from typing import Annotated

from pydantic import BaseModel, ConfigDict, StringConstraints

_NonBlank = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class FixtureInput(BaseModel):
    """One frozen capture and the runtime brief judged against it.

    ``people_sought`` names the class of position holders; ``subject``
    scopes the search; ``subject_label`` names the subject's role in this
    dataset. Together they render the brief exactly as the workers build it.
    """

    model_config = ConfigDict(extra="forbid")

    fixture: _NonBlank
    url: _NonBlank
    people_sought: _NonBlank
    subject_label: _NonBlank
    subject: _NonBlank
    final_url: str | None = None
    http_status: int | None = 200
    capture_error: str | None = None
