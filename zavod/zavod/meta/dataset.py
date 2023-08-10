import os
from banal import ensure_list, ensure_dict, hash_data, as_bool
from typing import Dict, Any, Optional, List, Set
from normality import slugify
from pathlib import Path
from urllib.parse import urljoin
from functools import cached_property
from datapatch import get_lookups, Lookup
from nomenklatura.dataset import Dataset as NKDataset
from nomenklatura.dataset import DataCatalog
from nomenklatura.util import datetime_iso

from zavod import settings
from zavod.meta.data import Data


class Dataset(NKDataset):
    def __init__(self, catalog: "DataCatalog[Dataset]", data: Dict[str, Any]):
        super().__init__(catalog, data)
        assert self.name == slugify(self.name, sep="_"), "Dataset name is invalid"
        self.catalog: "DataCatalog[Dataset]" = catalog  # type: ignore
        self.prefix: str = data.get("prefix", slugify(self.name, sep="-"))
        assert self.prefix == slugify(self.prefix, sep="-"), "Dataset prefix is invalid"
        if self.updated_at is None:
            self.updated_at = datetime_iso(settings.RUN_TIME)
        self.hidden: bool = as_bool(data.get("hidden", False))
        self.exports: Set[str] = set(data.get("exports", []))
        self.disabled: bool = as_bool(data.get("disabled", False))
        self.entry_point: Optional[str] = data.get("entry_point", None)

        self.load_db_uri: Optional[str] = data.get("load_db_uri", None)
        """Used to load the dataset into a database when doing a complete run."""
        if self.load_db_uri is not None:
            self.load_db_uri = os.path.expandvars(self.load_db_uri)
            if len(self.load_db_uri.strip()) == 0:
                self.load_db_uri = None

        self.config: Dict[str, Any] = ensure_dict(data.get("config", {}))
        _inputs = ensure_list(data.get("inputs", []))
        self._inputs: List[str] = [str(x) for x in _inputs]
        self._data = data
        self.base_path: Optional[Path] = None

        # TODO: this is for backward compatibility, get rid of it one day
        self._type: str = data.get("type", "source").lower().strip()

    @cached_property
    def input(self) -> Optional["Dataset"]:
        """The scopes of a dataset is the set of other datasets on which analysis or
        enrichment should be performed by the runner."""
        if not len(self._inputs):
            return None
        inputs = [self.catalog.require(s) for s in self._inputs]
        if len(inputs) == 1:
            return inputs[0]
        # Weird: if there are many scopes, we're making up a synthetic collection
        # to group them together so that we can build a store and view for them.
        names = sorted([i.name for i in inputs])
        key = hash_data(".".join(names))
        name = f"scope_{key}"
        if not self.catalog.has(name):
            data = {
                "name": name,
                "title": name,
                "datasets": names,
                "hidden": True,
                "exports": [],
            }
            scope = self.catalog.make_dataset(data)
            self.catalog.add(scope)
        return self.catalog.require(name)

    @cached_property
    def lookups(self) -> Dict[str, Lookup]:
        # TODO: debug mode
        return get_lookups(self._data.get("lookups", {}))

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
        data["exports"] = self.exports
        if self.data:
            data["data"] = self.data.to_dict()
        return data

    def to_opensanctions_dict(self) -> Dict[str, Any]:
        """Generate a backward-compatible metadata export."""
        data = self.to_dict()
        assert self._type in ("collection", "source", "external"), self._type
        data.pop("children", None)
        data.pop("datasets", None)
        data["type"] = self._type
        if self.is_collection:
            data["type"] = "collection"
            data["scopes"] = [s.name for s in self.leaves]
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
