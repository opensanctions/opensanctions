import os
from banal import ensure_list, ensure_dict, as_bool
from typing import Dict, Any, Optional, List, Set
from normality import slugify
from pathlib import Path
from urllib.parse import urljoin
from functools import cached_property
from datapatch import get_lookups, Lookup
from nomenklatura.dataset import Dataset as NKDataset
from nomenklatura.dataset import DataCatalog, DataCoverage
from nomenklatura.util import datetime_iso

from zavod import settings
from zavod.logs import get_logger
from zavod.meta.data import Data

log = get_logger(__name__)


class Dataset(NKDataset):
    def __init__(self, catalog: "DataCatalog[Dataset]", data: Dict[str, Any]):
        super().__init__(catalog, data)
        assert self.name == slugify(self.name, sep="_"), "Dataset name is invalid"
        self.catalog: "DataCatalog[Dataset]" = catalog  # type: ignore
        self.prefix: str = data.get("prefix", slugify(self.name, sep="-")).strip()
        assert self.prefix == slugify(self.prefix, sep="-"), (
            "Dataset prefix is invalid: %s" % self.prefix
        )
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

        self.load_db_uri: Optional[str] = data.get("load_db_uri", None)
        """Used to load the dataset into a database when doing a complete run."""
        if self.load_db_uri is not None:
            self.load_db_uri = os.path.expandvars(self.load_db_uri)
            if len(self.load_db_uri.strip()) == 0:
                self.load_db_uri = None

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

    def make_public_url(self, path: str) -> str:
        """Generate a public URL for a file within the dataset context."""
        url = urljoin(settings.DATASET_URL, f"{self.name}/")
        return urljoin(url, path)

    def to_dict(self) -> Dict[str, Any]:
        """Generate a metadata export, not including operational details."""
        data = super().to_dict()
        data["hidden"] = self.hidden
        data["disabled"] = self.disabled
        if self.data:
            data["data"] = self.data.to_dict()
        if self.full_dataset is not None:
            data["full_dataset"] = self.full_dataset
        return data

    def to_opensanctions_dict(self) -> Dict[str, Any]:
        """Generate a backward-compatible metadata export."""
        data = self.to_dict()
        assert self._type in ("collection", "source", "external"), self._type
        data.pop("resources", None)
        # data.pop("children", None)
        # data.pop("datasets", None)
        data["type"] = self._type
        if self.is_collection:
            # data["scopes"] = [s.name for s in self.leaves]
            data["sources"] = [s.name for s in self.leaves if s._type == "source"]
            data["externals"] = [s.name for s in self.leaves if s._type == "external"]
        else:
            collections = [
                p.name
                for p in self.catalog.datasets
                if self in p.datasets and p != self
            ]
            data["collections"] = collections
        if self.entry_point is not None:
            data["entry_point"] = self.entry_point
        return data
