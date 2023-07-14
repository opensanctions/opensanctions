import os
from typing import Any, Dict
from followthemoney.types import registry


class Data(object):
    """Data source specification."""

    def __init__(self, config: Dict[str, Any]) -> None:
        self.url = config.get("url")
        self.mode = config.get("mode")
        self.format = config.get("format")
        self.api_key = config.get("api_key")
        self.lang = registry.language.clean(config.get("lang"))
        if self.api_key is not None:
            self.api_key = os.path.expandvars(self.api_key)

    def to_dict(self) -> Dict[str, Any]:
        return {"url": self.url, "format": self.format, "mode": self.mode}
