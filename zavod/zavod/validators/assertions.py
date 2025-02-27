from typing import Dict, Any, Optional, cast

from zavod.context import Context
from zavod.entity import Entity
from zavod.exporters.statistics import Statistics
from zavod.meta.assertion import Assertion, Comparison, Metric
from zavod.store import View
from zavod.validators.common import BaseValidator


def compare_threshold(value: int, comparison: Comparison, threshold: int) -> bool:
    match comparison:
        case Comparison.GTE:
            return value >= threshold
        case Comparison.LTE:
            return value <= threshold
        case _:
            raise ValueError(f"Unknown comparison: {comparison}")


def get_value(stats: Dict[str, Any], assertion: Assertion) -> Optional[int]:
    match assertion.metric:
        case Metric.ENTITY_COUNT:
            match assertion.filter_attribute:
                case "schema":
                    items = stats["things"]["schemata"]
                    filter_key = "name"
                case "country":
                    items = stats["things"]["countries"]
                    filter_key = "code"
                case None:
                    assert assertion.filter_value is None
                    return cast(int, stats["things"]["total"])
                case _:
                    raise ValueError(
                        f"Unknown filter attribute: {assertion.filter_attribute}"
                    )
            items = [i for i in items if i[filter_key] == assertion.filter_value]
            if len(items) != 1:
                return None
            return cast(int, items[0]["count"])

        case Metric.ENTITIES_WITH_PROP_COUNT:
            items = stats["things"]["entities_with_prop"]
            items = [
                i
                for i in items
                # Filter value is a (schema_name, prop_name) tuple
                if assertion.filter_value
                and i["schema"] == assertion.filter_value[0]
                and i["property"] == assertion.filter_value[1]
            ]
            return cast(int, items[0]["count"]) if len(items) == 1 else None

        case Metric.COUNTRY_COUNT:
            return len(stats["things"]["countries"])

        case _:
            raise ValueError(f"Unknown metric: {assertion.metric}")


def check_assertion(
    context: Context, stats: Dict[str, Any], assertion: Assertion
) -> bool:
    """Returns true if the assertion is valid, false otherwise."""
    value = get_value(stats, assertion)
    log_fn = context.log.error if assertion.abort else context.log.warning
    if value is None:
        log_fn(f"Value not found for assertion {assertion}")
        return False
    if not compare_threshold(value, assertion.comparison, assertion.threshold):
        log_fn(f"Assertion failed for value {value}: {assertion}")
        return False
    return True


class AssertionsValidator(BaseValidator):
    """Aborts if any dataset assertion fails."""

    def __init__(self, context: Context, view: View) -> None:
        super().__init__(context, view)
        self.stats = Statistics()
        self.abort = False

    def feed(self, entity: Entity) -> None:
        self.stats.observe(entity)

    def finish(self) -> None:
        if len(self.context.dataset.assertions) == 0:
            self.context.log.warn("Dataset has no assertions.")

        for assertion in self.context.dataset.assertions:
            if not check_assertion(self.context, self.stats.as_dict(), assertion):
                self.abort = self.abort or assertion.abort
