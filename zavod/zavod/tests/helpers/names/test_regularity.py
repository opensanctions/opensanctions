from zavod.meta.dataset import Dataset
from zavod.entity import Entity

from zavod.helpers import (
    is_name_irregular,
)


def test_is_name_irregular(testdataset1: Dataset):
    org_data = {"id": "doe", "schema": "Organization", "properties": {}}
    org = Entity(testdataset1, org_data)
    person_data = {"id": "jon", "schema": "Person", "properties": {}}
    person = Entity(testdataset1, person_data)

    assert not is_name_irregular(org, "Org NPO")
    # Rejected chars
    assert is_name_irregular(org, "Org NPO, Org Charitable")
    # Nullwords
    assert is_name_irregular(org, "Unknown")
    # min_chars
    assert is_name_irregular(org, "a")  # too short
    assert not is_name_irregular(org, "A a")  # long enough
    # single_token_min_length
    assert is_name_irregular(org, "Aaa")  # too short
    assert not is_name_irregular(org, "Aaaa")  # long enough
    # Require space
    assert is_name_irregular(person, "Johnson")
    assert not is_name_irregular(org, "Johnson")
