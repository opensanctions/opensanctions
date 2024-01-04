from enum import Enum
from typing import Any, Dict, Generator, Optional
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


class Assertion(object):
    """Data assertion specification."""

    filter_attribute: Optional[str]
    filter_value: Optional[str]

    def __init__(
        self,
        metric: Metric,
        comparison: Comparison,
        threshold: int,
        filter_attribute: Optional[str],
        filter_value: Optional[str],
    ) -> None:
        self.metric = metric
        self.comparison = comparison
        self.threshold = threshold
        self.filter_attribute = filter_attribute
        self.filter_value = filter_value

    def __repr__(self) -> str:
        string = (
            f"<Assertion {self.metric.value} {self.comparison.value} {self.threshold}"
        )
        if self.filter_attribute is not None:
            string += f" filter: {self.filter_attribute}={self.filter_value}"
        string += ">"
        return string


def parse_filters(
    metric: Metric,
    comparison: Comparison,
    filter_attribute: str,
    config: Dict[str, Any],
) -> Generator[Assertion, None, None]:
    for key, value in config.items():
        threshold = int(type_require(registry.number, value))
        yield Assertion(metric, comparison, threshold, filter_attribute, key)


def parse_metrics(
    comparison: Comparison, config: Dict[str, Any]
) -> Generator[Assertion, None, None]:
    for key, value in config.items():
        match key:
            case "schema_entities":
                yield from parse_filters(
                    Metric.ENTITY_COUNT, comparison, "schema", value
                )
            case "country_entities":
                yield from parse_filters(
                    Metric.ENTITY_COUNT, comparison, "country", value
                )
            case "countries":
                threshold = int(type_require(registry.number, value))
                yield Assertion(Metric.COUNTRY_COUNT, comparison, threshold, None, None)
            case _:
                raise ValueError(f"Unknown metric: {key}")


def parse_assertions(config: Dict[str, Any]) -> Generator[Assertion, None, None]:
    for key, value in config.items():
        match key:
            case "min":
                comparison = Comparison.GT
            case "max":
                comparison = Comparison.LT
            case _:
                raise ValueError(f"Unknown assertion: {key}")
        yield from parse_metrics(comparison, value)
