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
    accepted_by: str
    accepted_at: str
    edited: bool
    """Whether the reviewer changed the original extraction before accepting it."""
    original_extraction: Extraction | None = None
    """The model output before reviewer edits. Only stored when it was edited."""


FixtureDataset = Dataset[CaseInputs, Extraction, CaseMeta]
