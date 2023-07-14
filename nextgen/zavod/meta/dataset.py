from typing import Dict, Any, Optional, Set
from normality import slugify
from functools import cached_property
from datapatch import get_lookups, Lookup
from nomenklatura.dataset import Dataset as NKDataset
from nomenklatura.dataset import DataCatalog
from nomenklatura.util import datetime_iso

from zavod import settings
from zavod.meta.data import Data


class Dataset(NKDataset):
    def __init__(self, catalog: DataCatalog["Dataset"], data: Dict[str, Any]):
        super().__init__(catalog, data)
        self.prefix: str = data.get("prefix", slugify(self.name, sep="-"))
        if self.updated_at is None:
            self.updated_at = datetime_iso(settings.RUN_TIME)
        self.hidden: bool = data.get("hidden", False)
        self.export: bool = data.get("export", True)
        self.disabled: bool = data.get("disabled", False)
        self._config = data

    @cached_property
    def lookups(self) -> Dict[str, Lookup]:
        # TODO: debug mode
        return get_lookups(self._config.get("lookups", {}))

    @cached_property
    def data(self) -> Optional[Data]:
        data_config = self._config.get("data", {})
        if not isinstance(data_config, dict) or not len(data_config):
            return None
        return Data(data_config)

    # TODO: move this to a separate module
    # @cached_property
    # def method(self):
    #     """Load the actual crawler code behind the dataset."""
    #     method = "crawl"
    #     module = self._config.get("module", None)
    #     if module is None:
    #         raise RuntimeError("The dataset has no entry point!")
    #     if ":" in module:
    #         module, method = module.rsplit(":", 1)
    #     # module = import_module(package)
    #     return getattr(module, method)

    def to_dict(self) -> Dict[str, Any]:
        """Generate a metadata export, not including operational details."""
        data = super().to_dict()
        data["hidden"] = self.hidden
        data["disabled"] = self.disabled
        data["export"] = self.export
        if self.data:
            data["data"] = self.data.to_dict()
        return data

    def to_opensanctions_dict(self) -> Dict[str, Any]:
        """Generate a backward-compatible metadata export."""
        data = self.to_dict()
        data["type"] = self._config.get("type", "source")
        if self.is_collection:
            data["type"] = "collection"
            data["scopes"] = [s.name for s in self.leaves]
            data["sources"] = [
                s.name for s in self.leaves if s._config["type"] == "source"
            ]
            data["externals"] = [
                s.name for s in self.leaves if s._config["type"] == "external"
            ]
        else:
            parents = [p.name for p in self.catalog.datasets if self in p.datasets]
            data["collections"] = parents
        # TODO: external !!
        return data
