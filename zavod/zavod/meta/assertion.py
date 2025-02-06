from enum import Enum
from typing import Any, Dict, Generator, Optional

from followthemoney import model


from followthemoney.types import registry
from nomenklatura.dataset.util import type_require


class Metric(Enum):
    ENTITY_COUNT = "entity_count"
    """Number of entities matching the filter in the dataset."""
    COUNTRY_COUNT = "country_count"
    """Number of distinct countries occurring in the dataset."""
    PROPERTY_VALUES_COUNT = "property_values_count"
    """Number of entities with property values matching the filter in the dataset."""


class Comparison(Enum):
    GTE = "gte"
    LTE = "lte"


class Assertion(object):
    """Data assertion specification."""

    filter_attribute: Optional[str]
    filter_value: Optional[Any]

    def __init__(
        self,
        metric: Metric,
        comparison: Comparison,
        threshold: int,
        filter_attribute: Optional[str],
        filter_value: Optional[Any],
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
            case "property_values" if isinstance(value, dict) and value != {}:
                for schema_name, props in value.items():
                    for prop_name, threshold_value in props.items():
                        schema = model.get(schema_name)
                        if schema is None:
                            raise ValueError(
                                f"Property value count assertion on unknown schema: {schema_name}:{prop_name}"
                            )
                        prop = schema.get(prop_name)
                        if prop is None:
                            raise ValueError(
                                f"Property value count assertion on unknown property: {schema_name}:{prop_name}"
                            )
                        yield Assertion(
                            Metric.PROPERTY_VALUES_COUNT,
                            comparison,
                            threshold_value,
                            "property_values",
                            # We don't just put a Property.qname in the shape of "Schema:name" here because we want to
                            # assert not on qnames, which are the canonical name of a property (e.g. Thing.country), but
                            # on property values in entities of a specific type, like Company.country.
                            (schema_name, prop_name),
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
                comparison = Comparison.GTE
            case "max":
                comparison = Comparison.LTE
            case _:
                raise ValueError(f"Unknown assertion: {key}")
        yield from parse_metrics(comparison, value)
