import pytest
from copy import deepcopy

from zavod.meta.assertion import parse_assertions, Comparison, Metric

CONFIG = {
    "min": {
        "schema_entities": {"Person": 1},
        "entities_with_prop": {"Person": {"name": 1}},
    },
    "max": {"countries": 1},
}


def test_parse_assertions():
    """Test assertions parsing. This doesn't actually test whether they work, that happens in test_validate."""

    assertions = list(parse_assertions(CONFIG))
    entity_count = assertions[0]
    assert entity_count.metric == Metric.SCHEMA_ENTITIES
    assert entity_count.config == {"Person": 1}
    assert entity_count.comparison == Comparison.GTE

    property_values_count = assertions[1]
    assert property_values_count.metric == Metric.ENTITIES_WITH_PROP_COUNT
    assert property_values_count.config == {"Person": {"name": 1}}
    assert property_values_count.comparison == Comparison.GTE

    country_count = assertions[2]
    assert country_count.metric == Metric.COUNTRY_COUNT
    assert country_count.config == 1
    assert country_count.comparison == Comparison.LTE

    config = deepcopy(CONFIG)
    # Should fail because "foo" is not a valid metric
    config["min"]["foo"] = config["min"].pop("schema_entities")
    with pytest.raises(ValueError):
        list(parse_assertions(config))
