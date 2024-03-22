from typing import Dict, Any, Optional, cast
from zavod.context import Context

from zavod.entity import Entity
from zavod.meta.assertion import Assertion, Comparison, Metric
from zavod.exporters.statistics import Statistics
from zavod.validators.common import BaseValidator
from zavod.store import View


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
                case _:
                    raise ValueError(
                        f"Unknown filter attribute: {assertion.filter_attribute}"
                    )
            items = [i for i in items if i[filter_key] == assertion.filter_value]
            if len(items) != 1:
                return None
            return cast(int, items[0]["count"])
        case Metric.COUNTRY_COUNT:
            return len(stats["things"]["countries"])
        case _:
            raise ValueError(f"Unknown metric: {assertion.metric}")


def check_assertion(
    context: Context, stats: Dict[str, Any], assertion: Assertion
) -> None:
    value = get_value(stats, assertion)
    if value is None:
        context.log.warning(f"Value not found for assertion {assertion}")
        return
    if not compare_threshold(value, assertion.comparison, assertion.threshold):
        context.log.warning(f"Assertion failed for value {value}: {assertion}")


class AssertionsValidator(BaseValidator):
    def __init__(self, context: Context, view: View) -> None:
        super().__init__(context, view)
        self.stats = Statistics()

    def feed(self, entity: Entity) -> None:
        self.stats.observe(entity)

    def finish(self) -> None:
        for assertion in self.context.dataset.assertions:
            check_assertion(self.context, self.stats.as_dict(), assertion)
