"""Typed model for the metadata zavod writes to a dataset's ``index.json``.

This is the *output* contract for a single dataset entry in the catalog. It
extends :class:`followthemoney.dataset.dataset.DatasetModel` — the loose schema
used to read any catalog, where nearly every field is optional — and tightens
it into what a published export must actually contain: ``version``,
``updated_at`` and ``last_export`` become required, ``resources`` use the
stricter published :class:`ResourceModel`, and it adds the operational fields
that only exist once a run has produced them (counts, ``last_change``, issue
levels, artifact URLs, ``result``).

The input side, :class:`zavod.meta.model.ZavodDatasetModel`, also extends
``DatasetModel``: it reads a dataset's description from its ``.yml`` metadata
file and carries knobs controlling how the crawler runs (e.g. scheduling and
memory consumption) that have no place on the output side.

The statistics-derived fields are required only for a successful run — a failed
run deliberately drops its statistics.

It is public API surface, so evolve it deliberately.
"""

from datetime import datetime
from typing import Annotated, Any, Literal, Self

from followthemoney.dataset.dataset import DatasetModel as FTMDatasetModel
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
    title: str | None = None
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


class CatalogDatasetModel(FTMDatasetModel):
    """A dataset entry in the OpenSanctions catalog and per-dataset ``index.json``.

    See the module docstring for how this tightens the FtM ``DatasetModel`` base.
    """

    # ── Tightened from the FtM base ───────────────────────────────────────────
    version: str
    updated_at: IsoDatetime
    last_export: IsoDatetime
    # ResourceModel is the stricter published form of the base's DataResource;
    # list invariance stops mypy from seeing this as a valid narrowing.
    resources: list[ResourceModel] = []  # type: ignore[assignment]
    """Downloadable data files for this dataset (excludes internal artifact files)."""

    # ── Zavod configuration carried into the export ───────────────────────────
    # TODO: this default ideally belongs only on the input (ZavodDatasetModel) side and passes through; kept here while we still build the index JSON by hand.
    entry_point: str | None = None
    disabled: bool
    hidden: bool
    # TODO: this default ideally belongs only on the input (ZavodDatasetModel) side and passes through; kept here while we still build the index JSON by hand.
    resolve: bool = True
    """Whether to run entity resolution on this dataset. Implicit default is True;
    only serialized when False (currently only 'maritime')."""
    # TODO: this default ideally belongs only on the input (ZavodDatasetModel) side and passes through; kept here while we still build the index JSON by hand.
    full_dataset: str | None = None
    # TODO: this default ideally belongs only on the input (ZavodDatasetModel) side and passes through; kept here while we still build the index JSON by hand.
    data: DataModel | None = None
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

    # ── Operational fields, only present after a run ──────────────────────────
    last_change: IsoDatetime | None = None
    """Timestamp of the most recent entity change in the dataset."""

    target_count: int | None = None
    """Number of entities flagged as targets."""

    issue_levels: dict[str, int] = {}
    """Count of issues per log level (e.g. {'warning': 3, 'error': 1}).
    Zero-filled before the first export. Keys are lowercase structlog level names."""

    issue_count: int
    """Total number of issues across all levels."""

    index_url: HttpUrl
    """Published URL of this dataset's index.json."""

    issues_url: HttpUrl
    """URL to the issues log artifact for the last export."""

    statistics_url: HttpUrl
    """URL to the statistics JSON artifact for the last export."""

    delta_url: HttpUrl | None = None
    """URL to the delta index for the most recent export that produced one.
    Not all exports produce a delta; None when absent."""

    result: Literal["success", "failure"] | None = None
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
