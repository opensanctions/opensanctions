from pathlib import Path
from functools import cached_property
from typing import TYPE_CHECKING, Dict, Any, Optional, List, Set

from rigour.time import datetime_iso
from banal import ensure_list, ensure_dict, as_bool
from datapatch import get_lookups, Lookup
from normality import slugify

from nomenklatura.dataset import DataCoverage
from nomenklatura.dataset import Dataset as NKDataset
from zavod import settings
from zavod.logs import get_logger
from zavod.meta.assertion import Assertion, parse_assertions, Comparison, Metric
from zavod.meta.data import Data
from zavod.meta.dates import DatesSpec
from zavod.meta.http import HTTP

if TYPE_CHECKING:
    from zavod.meta.catalog import ArchiveBackedCatalog

log = get_logger(__name__)


class Dataset(NKDataset):
    def __init__(self, data: Dict[str, Any]):
        super().__init__(data)
        assert self.name == slugify(self.name, sep="_"), "Dataset name is invalid"
        if len(self.summary or "") < 50:
            log.warning(
                "Dataset summary must be at least 50 chars.",
                dataset=self.name,
                summary=self.summary,
            )
        prefix_: Optional[str] = data.get("prefix", slugify(self.name, sep="-"))
        assert prefix_ is not None, "Dataset prefix cannot be None"
        assert prefix_ == slugify(prefix_, sep="-"), (
            "Dataset prefix is invalid: %s" % prefix_
        )
        self.prefix = prefix_.strip()

        if self.updated_at is None:
            self.updated_at = datetime_iso(settings.RUN_TIME)
        self.hidden: bool = as_bool(data.get("hidden", False))
        self.entry_point: Optional[str] = data.get("entry_point", None)
        """Code location for the crawler script"""

        self.exports: Set[str] = set(data.get("exports", []))
        """List of exporters to run on the dataset."""

        self.resolve: bool = as_bool(data.get("resolve", True))
        """Option to disable de-duplication mechanism."""

        self.full_dataset: Optional[str] = data.get("full_dataset", None)
        """The bulk full dataset for datasets that result from enrichment."""

        self.load_statements: bool = data.get("load_statements", False)
        """Used to load the dataset into a database when doing a complete run."""

        self.disabled: bool = as_bool(data.get("disabled", False))
        """Do not update the crawler at the moment."""
        # This will make disabled crawlers visible in the metadata:
        if self.disabled:
            if self.coverage is None:
                self.coverage = DataCoverage({})
            self.coverage.frequency = "never"

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

        self.ci_test: bool = as_bool(data.get("ci_test", True))
        """Whether this dataset should be automatically run in CI environments."""

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
        data["hidden"] = self.hidden
        data["disabled"] = self.disabled
        if not self.resolve:
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
        if self.entry_point is not None:
            data["entry_point"] = self.entry_point
        return data
