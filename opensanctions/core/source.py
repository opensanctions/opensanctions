import os
from importlib import import_module
from functools import cached_property
from typing import Set, Dict, Any
from followthemoney.types import registry
from nomenklatura.dataset import DataPublisher, DataCatalog

from opensanctions.core.dataset import Dataset


class SourceData(object):
    """Data source specification."""

    def __init__(self, config):
        self.url = config.get("url")
        self.mode = config.get("mode")
        self.format = config.get("format")
        self.api_key = config.get("api_key")
        self.lang = registry.language.clean(config.get("lang"))
        if self.api_key is not None:
            self.api_key = os.path.expandvars(self.api_key)

    def to_dict(self):
        return {"url": self.url, "format": self.format, "mode": self.mode}


class Source(Dataset):
    """A source to be included in OpenSanctions, backed by a crawler that can
    acquire and transform the data.
    """

    TYPE = "source"

    def __init__(self, catalog: DataCatalog, config: Dict[str, Any]):
        super().__init__(catalog, self.TYPE, config)
        self.disabled = config.get("disabled", False)
        self.entry_point = config.get("entry_point")
        self.data = SourceData(config.get("data", {}))
        self.publisher = DataPublisher(config.get("publisher", {}))

    @property
    def method(self):
        """Load the actual crawler code behind the dataset."""
        method = "crawl"
        package = self.entry_point
        if package is None:
            raise RuntimeError("The dataset has no entry point!")
        if ":" in package:
            package, method = package.rsplit(":", 1)
        module = import_module(package)
        return getattr(module, method)

    def to_dict(self):
        data = super().to_dict()
        parents = [p.name for p in self.catalog.datasets if self in p.datasets]
        data.update(
            {
                "url": self.url,
                "entry_point": self.entry_point,
                "disabled": self.disabled,
                "data": self.data.to_dict(),
                "publisher": self.publisher.to_dict(),
                "collections": parents,
            }
        )
        return data
