from typing import Dict, Any

from zavod.context import Context
from zavod.entity import Entity
from zavod.exporters.statistics import Statistics
from zavod.meta.assertion import Assertion, Comparison, Metric
from zavod.store import View
from zavod.validators.common import BaseValidator


def check_value(
    value: int | float, comparison: Comparison, threshold: int | float
) -> bool:
    match comparison:
        case Comparison.GTE:
            return value >= threshold
        case Comparison.LTE:
            return value <= threshold
        case _:
            raise ValueError(f"Unknown comparison: {comparison}")


def check_assertion(
    context: Context,
    stats: Dict[str, Any],
    assertion: Assertion,
    log_error: bool = True,
) -> bool:
    """Returns true if the assertion is valid, false otherwise."""

    log_fn = context.log.error if log_error else context.log.warning
    results_valid: list[bool] = []

    if assertion.metric == Metric.ENTITY_COUNT:
        threshold = int(assertion.config)
        value = stats["things"]["total"]
        valid = check_value(value, assertion.comparison, threshold)
        if not valid:
            log_fn(
                f"Assertion {assertion.metric} failed: {value} is not {assertion.comparison} threshold {threshold}"
            )
        results_valid.append(valid)

    elif assertion.metric == Metric.COUNTRY_COUNT:
        threshold = int(assertion.config)
        # stats["things"]["countries"] is a list of dictionaries that look like
        # [
        #   { "code": "us", "count": 100, label: "United States"},
        # ]
        # Here, we only care about the total count
        value = len(stats["things"]["countries"])
        valid = check_value(value, assertion.comparison, threshold)
        if not valid:
            log_fn(
                f"Assertion {assertion.metric} failed: {value} is not {assertion.comparison} threshold {threshold}"
            )
        results_valid.append(valid)

    elif assertion.metric == Metric.SCHEMA_ENTITIES:
        # stats["things"]["schemata"] is a list of dictionaries that look like
        # [
        #   { "name": "Person", "count": 100 },
        # ]
        stats_schema_to_count = {
            item["name"]: item["count"] for item in stats["things"]["schemata"]
        }

        for schema, threshold in assertion.config.items():
            value = stats_schema_to_count.get(schema, 0)
            valid = check_value(value, assertion.comparison, threshold)
            if not valid:
                log_fn(
                    f"Assertion {assertion.metric} failed for {schema}: {value} is not {assertion.comparison} threshold {threshold}"
                )
            results_valid.append(valid)

    elif assertion.metric == Metric.COUNTRY_ENTITIES:
        # stats["things"]["countries"] is a list of dictionaries that look like
        # [
        #   { "code": "us", "count": 100, label: "United States"},
        # ]
        stats_country_to_count = {
            item["code"]: item["count"] for item in stats["things"]["countries"]
        }

        for country, threshold in assertion.config.items():
            value = stats_country_to_count.get(country, 0)
            valid = check_value(value, assertion.comparison, threshold)
            if not valid:
                log_fn(
                    f"Assertion {assertion.metric} failed for {country}: {value} is not {assertion.comparison} threshold {threshold}"
                )
            results_valid.append(valid)

    elif assertion.metric == Metric.ENTITIES_WITH_PROP_COUNT:
        # stats["things"]["entities_with_prop"] is a list of dictionaries that look like
        # [
        #   { "schema": "Person", "property": "firstName", "count": 100 },
        # ]
        stats_entity_with_prop_to_count = {
            (item["schema"], item["property"]): item["count"]
            for item in stats["things"]["entities_with_prop"]
        }

        for schema, properties in assertion.config.items():
            for property, threshold in properties.items():
                key = (schema, property)
                value = stats_entity_with_prop_to_count.get(key, 0)
                valid = check_value(value, assertion.comparison, threshold)
                if not valid:
                    log_fn(
                        f"Assertion {assertion.metric} failed for {schema}.{property}: {value} is not {assertion.comparison} threshold {threshold}"
                    )
                results_valid.append(valid)

    else:
        raise ValueError(f"Unknown metric: {assertion.metric}")

    return all(results_valid)


class StatisticsAssertionsValidator(BaseValidator):
    """Validator that checks various asssertions that are based on dataset statistics."""

    def __init__(self, context: Context, view: View) -> None:
        super().__init__(context, view)
        self.stats = Statistics()
        self.abort = False

    def feed(self, entity: Entity) -> None:
        self.stats.observe(entity)

    def finish(self) -> None:
        if len(self.context.dataset.assertions) == 0:
            self.context.log.error("Dataset has no assertions.")

        for assertion in self.context.dataset.assertions:
            is_fatal = assertion.comparison == Comparison.GTE
            valid = check_assertion(
                self.context, self.stats.as_dict(), assertion, log_error=is_fatal
            )
            # Only min assertions should abort the dataset.
            if not valid and is_fatal:
                self.abort = True
