from typing import Any

from pydantic import BaseModel
from pydantic_evals import Dataset

# The extraction output is kept as a plain dict rather than the crawler's pydantic
# model so that fixtures survive model changes: fields dropped from the model are
# ignored by the evaluators, and fields added to the model are reported as unscored
# until the fixtures are re-exported.
Extraction = dict[str, Any]


class CaseInputs(BaseModel):
    source_file: str
    """Path of the source document, relative to the fixtures file."""
    source_label: str
    url: str | None = None


class CaseMeta(BaseModel):
    review_key: str
    accepted_at: str
    """When the review was accepted or last edited. Who accepted it is
    deliberately not recorded: it can inform which reviews become fixtures,
    but a fixture should not carry a reviewer's identity."""
    edited: bool
    """Whether the reviewer changed the original extraction before accepting it."""
    corrections: list[str] = []
    """One line per change the reviewer made before accepting, written at export
    time from the review's original extraction, e.g. `removed: Turkish Company`.
    The original extraction itself is not kept: it is model output under a
    prompt that no longer exists, so it goes stale and cannot be maintained."""
    rules: list[str] = []
    """Hand-written slugs for the rules a case was chosen to demonstrate, usually
    the rationale for a reviewer's correction (e.g. `unnamed-entity-skipped`).
    Rules that can be derived from the data are computed by `coverage` instead."""


FixtureDataset = Dataset[CaseInputs, Extraction, CaseMeta]
