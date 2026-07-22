"""Typed model for the metadata zavod writes to a dataset's ``index.json``.

This is the *output* contract for a single dataset entry in the catalog, and it
is deliberately distinct from the two *input* models it sits downstream of:

- ``followthemoney.dataset.dataset.DatasetModel`` is the loose, general schema
  used to parse any catalog. Nearly every field there is optional so it can read
  many shapes, so it cannot guard our exports.
- ``zavod.meta.model.ZavodDatasetModel`` reads a dataset's description from its
  ``.yml`` metadata file. Beyond the descriptive fields it also carries knobs
  that control how the crawler runs — e.g. scheduling and memory consumption —
  which have no place on the output side. It carries none of the operational
  run fields (counts, ``last_change``, artifact URLs, ``result``) that only
  come into existence after an export.

``CatalogDatasetModel`` is a flat, standalone model rather than a subclass of
either of the above, for two reasons: (a) it adds the operational fields that
only exist once a run has produced them, and (b) it is more restrictive than the
input models because it is public API surface, which we want to evolve
deliberately.
"""

from datetime import datetime
from typing import Annotated, Any, Literal, Optional, Self

from followthemoney.dataset.coverage import DataCoverage
from followthemoney.dataset.publisher import DataPublisher
from pydantic import (
    BaseModel,
    HttpUrl,
    field_validator,
    model_serializer,
    model_validator,
)
from pydantic.functional_serializers import PlainSerializer

from zavod.meta.model import DataModel

# Very explicitly enforce the datetime format we use in the catalog
# (ISO 8601 without timezone).
IsoDatetime = Annotated[
    datetime,
    PlainSerializer(lambda dt: dt.strftime("%Y-%m-%dT%H:%M:%S"), return_type=str),
]


class ResourceModel(BaseModel):
    """A downloadable data file attached to a dataset."""

    name: str
    """Filename, e.g. 'entities.ftm.json' or 'targets.csv'."""
    path: str
    """Deprecated — always identical to `name`. Kept for backwards compatibility with older consumers."""
    url: HttpUrl
    """Full published URL, e.g. 'https://data.opensanctions.org/datasets/20260429/us_ofac_sdn/entities.ftm.json'."""
    checksum: str
    """SHA1 hex digest of the file content."""
    mime_type: str
    """MIME type, e.g. 'application/json+ftm' or 'text/plain'."""
    mime_type_label: str
    """Generic format label derived from the MIME type, e.g. 'FollowTheMoney Entities' or 'Plain text'."""
    title: Optional[str] = None
    """Description of what this specific file contains, e.g. 'FollowTheMoney entities' or 'Target names text file'.
    Absent for raw source files published without a title (e.g. pass-through .xlsx files)."""
    size: int
    """File size in bytes."""

    @model_validator(mode="after")
    def validate_name_equals_path(self) -> Self:
        if self.name != self.path:
            raise ValueError(
                f"resource name {self.name!r} does not match path {self.path!r}"
            )
        return self


class CatalogDatasetModel(BaseModel):
    """Typed representation of a dataset entry in the OpenSanctions catalog and
    per-dataset ``index.json``.

    See the module docstring for how this output model relates to the
    ``DatasetModel`` / ``ZavodDatasetModel`` input models.
    """

    name: str
    title: str
    summary: Optional[str] = None
    description: Optional[str] = None
    url: Optional[HttpUrl] = None
    version: str
    tags: list[str] = []
    publisher: Optional[DataPublisher] = None
    """None for collections, which aggregate sources rather than having their own publisher."""
    coverage: Optional[DataCoverage] = None
    children: set[str] = set()
    """Internal representation of the datasets included in this collection. Prefer `datasets` for external use."""
    deprecation: Optional[str] = None
    """Markdown explanation of why the dataset was deprecated and what happened to it.
    Only present when `deprecated` is True, e.g. 'The source API was shut down in April 2025...'"""
    deprecated: bool
    """Whether this dataset is deprecated (i.e. no longer actively maintained or updated)."""
    entry_point: Optional[str] = None
    disabled: bool
    hidden: bool
    resolve: bool = True
    """Whether to run entity resolution on this dataset. Implicit default is True;
    only serialized when False (currently only 'maritime')."""
    full_dataset: Optional[str] = None

    data: Optional[DataModel] = None
    """Crawler source metadata (URL, format, mode). None for collections and some externals."""

    # ── Fields computed for the catalog export ────────────────────────────────
    type: Literal["collection", "source", "external"]
    """Dataset kind: 'collection' groups sources; 'source' is a crawled dataset;
    'external' is an externally-managed dataset."""

    collections: list[str] = []
    """For sources/externals: names of all collections that include this dataset,
    populated by scanning the catalog. Empty for collections themselves."""

    datasets: list[str] = []
    """The public-facing list of datasets included in this collection — always identical to `children`.
    This is the field documented and intended for external consumers; `children` is the internal representation."""

    updated_at: IsoDatetime
    last_export: IsoDatetime
    last_change: Optional[IsoDatetime] = None
    """Timestamp of the most recent entity change in the dataset."""

    entity_count: Optional[int] = None
    thing_count: Optional[int] = None
    target_count: Optional[int] = None
    """Number of entities flagged as targets."""

    issue_levels: dict[str, int] = {}
    """Count of issues per log level (e.g. {'warning': 3, 'error': 1}).
    Zero-filled before the first export. Keys are lowercase structlog level names."""

    issue_count: int
    """Total number of issues across all levels."""

    resources: list[ResourceModel] = []
    """Downloadable data files for this dataset (excludes internal artifact files)."""

    index_url: HttpUrl
    """Published URL of this dataset's index.json."""

    issues_url: HttpUrl
    """URL to the issues log artifact for the last export."""

    statistics_url: HttpUrl
    """URL to the statistics JSON artifact for the last export."""

    delta_url: Optional[HttpUrl] = None
    """URL to the delta index for the most recent export that produced one.
    Not all exports produce a delta; None when absent."""

    result: Optional[Literal["success", "failure"]] = None
    """Outcome of the last export run."""

    @field_validator("tags")
    @classmethod
    def validate_tags(cls, v: list[str]) -> list[str]:
        for tag in v:
            if " " in tag:
                raise ValueError(f"tag {tag!r} must not contain spaces")
        return v

    @model_validator(mode="after")
    def validate_collection_has_children(self) -> Self:
        if self.type == "collection" and not self.children:
            raise ValueError(
                f"collection {self.name!r} must have at least one child dataset"
            )
        return self

    @model_validator(mode="after")
    def validate_successful_run_has_statistics(self) -> Self:
        # A failed run deliberately drops its statistics, so the derived fields
        # are absent by design. A successful run must carry them.
        if self.result == "success":
            missing = [
                name
                for name in (
                    "entity_count",
                    "thing_count",
                    "target_count",
                    "last_change",
                )
                if getattr(self, name) is None
            ]
            if missing:
                raise ValueError(
                    f"successful run of {self.name!r} is missing statistics fields: {missing}"
                )
        return self

    @model_serializer(mode="wrap")
    def _serialize(self, handler: Any) -> dict[str, Any]:
        d: dict[str, Any] = handler(self)
        # Mirror zavod's serialization: resolve is omitted when True (currently only 'maritime' has False).
        if self.resolve is True:
            d.pop("resolve", None)
        return d
