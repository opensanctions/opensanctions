import os
from typing import Any, Dict
from followthemoney.types import registry
from nomenklatura.dataset.util import type_check


class Data(object):
    """Data source specification."""

    def __init__(self, config: Dict[str, Any]) -> None:
        self.url = type_check(registry.url, config.get("url"))
        self.mode = type_check(registry.string, config.get("mode"))
        self.format = type_check(registry.string, config.get("format"))
        self.api_key = type_check(registry.string, config.get("api_key"))
        self.lang = type_check(registry.language, config.get("lang"))
        if self.api_key is not None:
            self.api_key = os.path.expandvars(self.api_key)

    def to_dict(self) -> Dict[str, Any]:
        data = {"url": self.url, "format": self.format}
        if self.mode is not None:
            data["mode"] = self.mode
        return data
