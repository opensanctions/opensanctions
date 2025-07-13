from pathlib import Path
from functools import cached_property
from typing import TYPE_CHECKING, Dict, Any, Optional, List, Set

from pydantic import field_validator
from rigour.time import datetime_iso
from banal import ensure_list, ensure_dict
from datapatch import get_lookups, Lookup
from normality import slugify
from followthemoney.dataset import Dataset as FollowTheMoneyDataset
from followthemoney.dataset.dataset import DatasetModel as FollowTheMoneyDatasetModel
from followthemoney.dataset.coverage import DataCoverage

from zavod import settings
from zavod.logs import get_logger
from zavod.meta.assertion import Assertion, parse_assertions, Comparison, Metric
from zavod.meta.data import Data
from zavod.meta.dates import DatesSpec
from zavod.meta.http import HTTP

if TYPE_CHECKING:
    from zavod.meta.catalog import ArchiveBackedCatalog

log = get_logger(__name__)


class OpenSanctionsDatasetModel(FollowTheMoneyDatasetModel):
    entry_point: Optional[str] = None
    """Code location for the crawler script"""

    prefix: str

    disabled: bool = False
    """Do not update the crawler at the moment."""

    hidden: bool = False
    """Do not show this dataset in the website and other UIs."""

    resolve: bool = True
    """Resolve entities in this dataset to canonical IDs."""

    exports: Set[str] = set()

    ci_test: bool = True
    """Whether this dataset should be automatically run in CI environments."""

    @field_validator("prefix", mode="after")
    @classmethod
    def check_prefix(cls, prefix: str) -> str:
        if prefix != slugify(prefix, sep="-"):
            msg = f"Dataset prefix is invalid: {prefix!r}. Must be a slugified string"
            raise ValueError(msg)
        prefix = prefix.strip("-")
        if len(prefix) == 0:
            raise ValueError("Dataset prefix cannot be empty")
        return prefix


class Dataset(FollowTheMoneyDataset):
    Model = OpenSanctionsDatasetModel

    def __init__(self, data: Dict[str, Any]):
        super().__init__(data)
        self.model = self.Model.model_validate(data)
        if len(self.model.summary or "") < 50:
            log.warning(
                "Dataset summary must be at least 50 chars.",
                dataset=self.name,
                summary=self.model.summary,
            )

        if self.updated_at is None:
            self.updated_at = datetime_iso(settings.RUN_TIME)

        self.prefix = self.model.prefix

        self.full_dataset: Optional[str] = data.get("full_dataset", None)
        """The bulk full dataset for datasets that result from enrichment."""

        self.load_statements: bool = data.get("load_statements", False)
        """Used to load the dataset into a database when doing a complete run."""

        # This will make disabled crawlers visible in the metadata:
        if self.model.disabled:
            if self.model.coverage is None:
                self.model.coverage = DataCoverage()
            self.model.coverage.frequency = "never"

        self.config: Dict[str, Any] = ensure_dict(data.get("config", {}))
        _inputs = ensure_list(data.get("inputs", []))
        self.inputs: List[str] = [str(x) for x in _inputs]
        """List of other datasets that this dataset depends on as processing inputs."""

        self._data = data
        self.base_path: Optional[Path] = None

        # TODO: this is for backward compatibility, get rid of it one day
        _type = "collection" if self.is_collection else "source"
        self._type: str = data.get("type", _type).lower().strip()

        self.assertions: List[Assertion] = list(
            parse_assertions(data.get("assertions", {}))
        )
        """
        List of assertions which should be considered warnings if they fail.

        Configured as follows:

        ```yaml
          min:
            schema_entities:
              Person: 160000
              Position: 10000
            country_entities:
              us: 20000
              cn: 8000
            countries: 19
          max:
            schema_entities:
              Person: 180000
        ```
        """

        self.assertions.append(
            # At least one entity in dataset
            Assertion(Metric.ENTITY_COUNT, Comparison.GTE, 1, None, None)
        )

        self.http: HTTP = HTTP(data.get("http", {}))
        """HTTP configuration for this dataset."""

        self.dates: DatesSpec = DatesSpec(data.get("dates", {}))
        """Date parsing configuration for this dataset."""

    @cached_property
    def lookups(self) -> Dict[str, Lookup]:
        config = self._data.get("lookups", {})
        return get_lookups(config, debug=settings.DEBUG)

    @cached_property
    def data(self) -> Optional[Data]:
        data_config = self._data.get("data", {})
        if not isinstance(data_config, dict) or not len(data_config):
            return None
        return Data(data_config)

    def to_dict(self) -> Dict[str, Any]:
        """Generate a metadata export, not including operational details."""
        data = super().to_dict()
        data["hidden"] = self.model.hidden
        data["disabled"] = self.model.disabled
        if not self.model.resolve:
            data["resolve"] = False
        if self.data:
            data["data"] = self.data.to_dict()
        if self.full_dataset is not None:
            data["full_dataset"] = self.full_dataset
        return data

    def to_opensanctions_dict(self, catalog: "ArchiveBackedCatalog") -> Dict[str, Any]:
        """Generate a backward-compatible metadata export."""
        data = self.to_dict()
        assert self._type in ("collection", "source", "external"), self._type
        data.pop("resources", None)
        data.pop("version", None)
        # data.pop("children", None)
        # data.pop("datasets", None)
        data["type"] = self._type
        if not self.is_collection:
            collections = [
                p.name for p in catalog.datasets if self in p.datasets and p != self
            ]
            data["collections"] = collections
        if self.model.entry_point is not None:
            data["entry_point"] = self.model.entry_point
        return data
