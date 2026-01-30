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


def is_assertion_fatal(assertion: Assertion) -> bool:
    return assertion.comparison == Comparison.GTE


def check_assertion(
    context: Context,
    stats: Dict[str, Any],
    assertion: Assertion,
) -> bool:
    """Returns true if the assertion is valid, false otherwise."""

    log_fn = context.log.error if is_assertion_fatal(assertion) else context.log.warning
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

    elif assertion.metric == Metric.PROPERTY_FILL_RATE:
        # We handle that in a separate validator below because it's a special cookie
        # that doesn't just want the Statistics
        pass
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
            valid = check_assertion(self.context, self.stats.as_dict(), assertion)
            # Only min assertions should abort the dataset.
            if not valid and is_assertion_fatal(assertion):
                self.abort = True


class PropertyFillRateAssertionsValidator(BaseValidator):
    """Warns if the fill rate of a property is below a threshold.

    The reason this is implemented as a separate validator is that it doesn't just want the Statistics,
    but the actual entities to compute the fill rate. Putting it in the AssertionsValidator would have
    made it too complex, so instead I chose to spread the assertions functionality across multiple validators.
    """

    # schema, property -> (total, with_prop_set)
    # In the end, we do fill_rate = with_prop_set / total
    _counts_by_filter: dict[tuple[str, str], tuple[int, int]] = {}

    # For convenience, store property_fill_rate assertions in a list.
    # This should only be up to two (one for min, one for max)
    _assertions: list[Assertion] = []

    def __init__(self, context: Context, view: View) -> None:
        super().__init__(context, view)

        self._assertions = [
            assertion
            for assertion in self.context.dataset.assertions
            if assertion.metric == Metric.PROPERTY_FILL_RATE
        ]

        for assertion in self._assertions:
            for schema, properties in assertion.config.items():
                for property, threshold in properties.items():
                    self._counts_by_filter[(schema, property)] = (0, 0)

    def feed(self, entity: Entity) -> None:
        for (schema, prop), (total, with_prop_set) in self._counts_by_filter.items():
            if entity.schema.is_a(schema):
                self._counts_by_filter[(schema, prop)] = (
                    total + 1,
                    with_prop_set + (1 if entity.has(prop) else 0),
                )

    def finish(self) -> None:
        for assertion in self._assertions:
            log_fn = (
                self.context.log.error
                if is_assertion_fatal(assertion)
                else self.context.log.warning
            )

            for schema, properties in assertion.config.items():
                for property, threshold in properties.items():
                    key = (schema, property)
                    total, with_prop_set = self._counts_by_filter[key]
                    # Avoid division by zero
                    fill_rate = with_prop_set / (total or 1)
                    valid = check_value(fill_rate, assertion.comparison, threshold)
                    if not valid:
                        log_fn(
                            f"Assertion {assertion.metric} failed for {schema}.{property}: {fill_rate} is not {assertion.comparison} threshold {threshold}"
                        )
                        # Only fatal assertions should abort the dataset.
                        if is_assertion_fatal(assertion):
                            self.abort = True
