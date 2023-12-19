from enum import Enum
from typing import Any, Dict
from followthemoney.types import registry
from nomenklatura.dataset.util import type_require


class Metric(Enum):
    ENTITY_COUNT = "entity_count"
    """Number of entities matching the filter in the dataset."""
    COUNTRY_COUNT = "country_count"
    """Number of distinct countries occurring in the dataset."""


class Comparison(Enum):
    GT = "gt"
    LT = "lt"


class Action(Enum):
    WARN = "warn"
    """Emit a warning-level log message."""
    FAIL = "fail"
    """Fail the job and do not complete producing the dataset."""


class Assertion(object):
    """Data assertion specification."""

    def __init__(self, config: Dict[str, Any]) -> None:
        self.metric = Metric(type_require(registry.string, config.get("metric")))
        comparison_ = type_require(registry.string, config.get("comparison"))
        self.comparison = Comparison(comparison_)
        self.threshold = int(type_require(registry.number, config.get("threshold")))
        action_ = type_require(registry.string, config.get("action"))
        self.action = Action(action_)
        if self.metric == Metric.ENTITY_COUNT:
            filter = config.get("filter", {})
            self.filter_attribute = type_require(registry.string, filter.get("attribute"))
            self.filter_value = type_require(registry.string, filter.get("value"))
        else:
            self.filter_attribute = None
            self.filter_value = None

    def __repr__(self) -> str:
        string = f"<Assertion {self.metric.value} {self.comparison.value} {self.threshold}"
        if self.filter_attribute is not None:
            string  += f" filter: {self.filter_attribute}={self.filter_value}"
        string += ">"
        return string
