import pytest
from copy import deepcopy
from nomenklatura.exceptions import MetadataException

from zavod.meta.assertion import Assertion, Comparison, Metric

ENTITY_COUNT = {
    "metric": "entity_count",
    "comparison": "gt",
    "threshold": 1,
    "filter": {
        "attribute": "schema",
        "value": "Person"
    }
}
COUNTRY_COUNT = {
    "metric": "country_count",
    "comparison": "lt",
    "threshold": 1
}


def test_assertion():
    entity_count = Assertion(ENTITY_COUNT)
    assert entity_count.metric == Metric.ENTITY_COUNT
    assert entity_count.filter_attribute == "schema"
    assert entity_count.comparison == Comparison.GT

    country_count = Assertion(COUNTRY_COUNT)
    assert country_count.metric == Metric.COUNTRY_COUNT
    assert country_count.filter_attribute is None
    assert country_count.comparison == Comparison.LT

    with pytest.raises(MetadataException) as e_info:
        Assertion({})

    config = deepcopy(ENTITY_COUNT)
    config["metric"] = "foo"
    with pytest.raises(ValueError) as e_info:
        Assertion(config)

    config = deepcopy(ENTITY_COUNT)
    config["comparison"] = "gte"
    with pytest.raises(ValueError) as e_info:
        Assertion(config)

    config = deepcopy(ENTITY_COUNT)
    del config["filter"]
    with pytest.raises(MetadataException) as e_info:
        Assertion(config)