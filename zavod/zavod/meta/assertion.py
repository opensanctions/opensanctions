from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Generator


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


def parse_assertions(config: Dict[str, Any]) -> Generator[Assertion, None, None]:
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
