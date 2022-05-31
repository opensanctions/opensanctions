import os
from importlib import import_module
from functools import cached_property
from typing import Set

from opensanctions.core.dataset import Dataset, DatasetPublisher


class SourceData(object):
    """Data source specification."""

    def __init__(self, config):
        self.url = config.get("url")
        self.mode = config.get("mode")
        self.format = config.get("format")
        self.api_key = config.get("api_key")
        if self.api_key is not None:
            self.api_key = os.path.expandvars(self.api_key)

    def to_dict(self):
        return {"url": self.url, "format": self.format, "mode": self.mode}


class Source(Dataset):
    """A source to be included in OpenSanctions, backed by a crawler that can
    acquire and transform the data.
    """

    TYPE = "source"

    def __init__(self, file_path, config):
        super().__init__(self.TYPE, file_path, config)
        self.url = config.get("url", "")
        self.disabled = config.get("disabled", False)
        self.entry_point = config.get("entry_point")
        self.data = SourceData(config.get("data", {}))
        self.publisher = DatasetPublisher(config.get("publisher", {}))

    @cached_property
    def sources(self) -> Set["Source"]:
        return set([self])

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
        data.update(
            {
                "url": self.url,
                "entry_point": self.entry_point,
                "disabled": self.disabled,
                "data": self.data.to_dict(),
                "publisher": self.publisher.to_dict(),
                "collections": self.collections,
            }
        )
        return data
