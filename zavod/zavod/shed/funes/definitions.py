"""Validation of the funes brief carried in zavod dataset metadata.

A funes dataset is a zavod dataset whose ``config`` block carries a
``funes`` mapping: the inspection brief (``people_sought``,
``subject_label``), the ``revisit_interval_days`` a hit must age before
its candidate is due again, and the ``subjects`` with their known seed
URLs. ``dataset_definition`` validates that mapping into a
``DatasetDefinition``, the input of the catalogue sync
(``zavod.shed.funes.catalogue``).
"""

from pydantic import BaseModel, ConfigDict, Field, field_validator
from rigour.urls import clean_url

from zavod.meta import Dataset


class SubjectDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    urls: list[str] = Field(default_factory=list)
    """May be empty while a future spider discovers pages for the subject."""

    @field_validator("urls")
    @classmethod
    def reject_noncanonical_urls(cls, urls: list[str]) -> list[str]:
        """The brief is hand-curated: a URL ``clean_url`` would rewrite or
        reject is fixed in the YAML, not repaired here."""
        for url in urls:
            if clean_url(url) != url:
                raise ValueError(f"invalid or non-canonical url: {url!r}")
        return urls


class DatasetDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    people_sought: str
    """Names the class of position holders the dataset is after."""
    subject_label: str
    """Names the subject's role in the inspection brief (e.g. Organization,
    Court, Sending country)."""
    revisit_interval_days: int = Field(gt=0)
    """How many days a hit attempt must age before its candidate is due
    for a new run."""

    subjects: list[SubjectDefinition] = Field(min_length=1)
    """Non-empty: candidates are URLs of subjects, and the pipeline
    discovers URLs, never subjects."""


def dataset_definition(dataset: Dataset) -> DatasetDefinition | None:
    """The validated ``config.funes`` brief of a dataset, or ``None`` if
    it carries none."""
    brief = dataset.config.get("funes")
    if brief is None:
        return None
    if not isinstance(brief, dict):
        raise ValueError(
            f"config.funes must be a mapping, got {type(brief).__name__} "
            f"in dataset {dataset.name}"
        )
    return DatasetDefinition(name=dataset.name, **brief)
