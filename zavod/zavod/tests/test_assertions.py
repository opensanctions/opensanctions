import pytest
from copy import deepcopy
from nomenklatura.exceptions import MetadataException

from zavod.meta.assertion import parse_assertions, Comparison, Metric

CONFIG = {
    "min": {
        "schema_entities": {
            "Person": 1
        },
    },
    "max": {
        "countries": 1
    }
}


def test_parse_assertions():
    assertions = list(parse_assertions(CONFIG))
    entity_count = assertions[0]
    assert entity_count.metric == Metric.ENTITY_COUNT
    assert entity_count.filter_attribute == "schema"
    assert entity_count.comparison == Comparison.GT

    country_count = assertions[1]
    assert country_count.metric == Metric.COUNTRY_COUNT
    assert country_count.filter_attribute is None
    assert country_count.comparison == Comparison.LT

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
