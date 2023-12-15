from enum import Enum
from typing import Any, Dict
from followthemoney.types import registry
from nomenklatura.dataset.util import type_require


class Comparison(Enum):
    GT = "GT"
    LT = "LT"


class Action(Enum):
    WARN = "WARN"
    """Emit a warning-level log message."""
    FAIL = "FAIL"
    """Fail the job and do not complete producing the dataset."""


class Assertion(object):
    """Data assertion specification."""

    def __init__(self, config: Dict[str, Any]) -> None:
        self.comparison = Comparison[config.get("comparison")]
        self.threshold = int(type_require(registry.number, config.get("threshold")))
        self.action = Action[config.get("action")]
        filter = config.get("filter")
        self.filter_attribute = type_require(registry.string, filter.get("attribute"))
        self.filter_value = type_require(registry.string, filter.get("value"))

    def __repr__(self) -> str:
        return (
            f"<Assertion {self.comparison.value} {self.threshold} "
            f"filter: {self.filter_attribute}={self.filter_value}>"
        )
