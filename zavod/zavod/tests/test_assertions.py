import pytest
from copy import deepcopy

from zavod.meta.assertion import parse_assertions, Comparison, Metric

CONFIG = {
    "min": {
        "schema_entities": {"Person": 1},
        "property_values": {"Person": {"name": 1}},
    },
    "max": {"countries": 1},
}


def test_parse_assertions():
    assertions = list(parse_assertions(CONFIG))
    entity_count = assertions[0]
    assert entity_count.metric == Metric.ENTITY_COUNT
    assert entity_count.filter_attribute == "schema"
    assert entity_count.comparison == Comparison.GTE

    property_values_count = assertions[1]
    assert property_values_count.metric == Metric.PROPERTY_VALUES_COUNT
    assert property_values_count.filter_attribute == "property_values"
    assert property_values_count.filter_value == ("Person", "name")
    assert property_values_count.comparison == Comparison.GTE

    country_count = assertions[2]
    assert country_count.metric == Metric.COUNTRY_COUNT
    assert country_count.filter_attribute is None
    assert country_count.comparison == Comparison.LTE

    config = deepcopy(CONFIG)
    config["min"]["foo"] = config["min"].pop("schema_entities")
    with pytest.raises(ValueError):
        list(parse_assertions(config))

    config = deepcopy(CONFIG)
    config["min"]["schema_entities"] = 1
    with pytest.raises(Exception):
        list(parse_assertions(config))

    config = deepcopy(CONFIG)
    config["min"]["schema_entities"]["Person"] = "foo"
    with pytest.raises(Exception):
        list(parse_assertions(config))

    config = deepcopy(CONFIG)
    config["whatever"] = config.pop("min")
    with pytest.raises(ValueError):
        list(parse_assertions(config))


def test_parse_property_values_count_unknown_property_name_or_schema():
    config = deepcopy(CONFIG)

    config["min"]["property_values"] = {"Person": {"bogusProperty": 1}}
    with pytest.raises(ValueError):
        list(parse_assertions(config))

    config["min"]["property_values"] = {"bogusSchema": {"name": 1}}
    with pytest.raises(ValueError):
        list(parse_assertions(config))
