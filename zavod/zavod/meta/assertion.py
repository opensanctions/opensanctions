from copy import deepcopy
from dataclasses import dataclass
from enum import Enum
from typing import Any
from collections.abc import Generator


class Metric(Enum):
    ENTITY_COUNT = "entity_count"

    SCHEMA_ENTITIES = "schema_entities"
    """Number of entities with of a given schema in the dataset."""

    COUNTRY_ENTITIES = "country_entities"
    """Number of entities with a given country in the dataset."""

    COUNTRY_COUNT = "countries"
    """Number of distinct countries occurring in the dataset."""

    ENTITIES_WITH_PROP_COUNT = "entities_with_prop"
    """Number of entities with property values matching the filter in the dataset."""

    PROPERTY_FILL_RATE = "property_fill_rate"
    """Fill rate of a property for a given schema in the dataset."""

    def __str__(self) -> str:
        return self.value


class Comparison(Enum):
    GTE = "gte"
    LTE = "lte"

    def __str__(self) -> str:
        match self:
            case Comparison.GTE:
                return ">="
            case Comparison.LTE:
                return "<="
            case _:
                raise ValueError(f"Unknown comparison: {self}")


@dataclass
class Assertion:
    """Assertions that fail or warn on dataset exports.

    This is only a dataclass that holds the configuration. The heavy lifting is done in the AssertionsValidator.
    """

    # gte (i.e. at minimum X entities present) fails the export,
    # lte just warns. This behaivor is implemented in the assertions validator.
    comparison: Comparison

    metric: Metric

    # Configuration is read in the validator.
    # For some metrics, it's just a single value, for others it's a more complex dictionary.
    config: Any

    def __repr__(self) -> str:
        return (
            f"<Assertion {self.metric.value} {self.comparison.value} {self.config!r}>"
        )


def merge_assertions_config(
    base: dict[str, Any], override: dict[str, Any]
) -> dict[str, Any]:
    """Deep-merge two assertion config dicts, with `override` winning at the leaf.

    Nested dicts (comparison -> metric -> schema -> property) are merged
    recursively; any non-dict value in `override` replaces the base value.
    """
    result = deepcopy(base)
    for key, value in override.items():
        existing = result.get(key)
        if isinstance(existing, dict) and isinstance(value, dict):
            result[key] = merge_assertions_config(existing, value)
        else:
            result[key] = deepcopy(value)
    return result


def parse_assertions(config: dict[str, Any]) -> Generator[Assertion, None, None]:
    for key, metrics_config in config.items():
        match key:
            case "min":
                comparison = Comparison.GTE
            case "max":
                comparison = Comparison.LTE
            case _:
                raise ValueError(f"Unknown assertion: {key}")

        for metric, config in metrics_config.items():
            yield Assertion(comparison=comparison, metric=Metric(metric), config=config)
