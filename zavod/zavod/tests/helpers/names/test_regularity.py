from zavod.meta.dataset import Dataset
from zavod.entity import Entity

from zavod.helpers import (
    is_name_irregular,
    check_name_regularity,
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

    # min_length
    assert is_name_irregular(org, "a")  # too short
    assert not is_name_irregular(org, "A a")  # long enough
    # not too short because min_length doesn't apply to dense scripts like Hangul for Korean
    assert not is_name_irregular(org, "벡셀")

    # single_token_min_length
    assert is_name_irregular(org, "Aaa")  # too short
    assert not is_name_irregular(org, "Aaaa")  # long enough

    # Require space
    assert is_name_irregular(person, "Johnson")
    assert not is_name_irregular(org, "Johnson")
    # no spaces but not irregular for this script
    assert not is_name_irregular(person, "김정은")


def test_suggest_person_single_token(testdataset1: Dataset):
    # testdataset1 has suggest_person_single_token: true
    person_data = {"id": "jon", "schema": "Person", "properties": {}}
    person = Entity(testdataset1, person_data)

    # Single token -> suggests weakAlias
    reg = check_name_regularity(person, "Johnson")
    assert reg.is_irregular
    assert reg.suggested_prop == "weakAlias"

    # Prefix stripped to single token -> suggests weakAlias
    reg = check_name_regularity(person, "Mr. Johnson")
    assert reg.is_irregular
    assert reg.suggested_prop == "weakAlias"

    # Multi-token -> no suggestion from this heuristic
    reg = check_name_regularity(person, "John Smith")
    assert not reg.is_irregular


def test_suggest_weak_alias_uppercase_org_single_token_shorter_than(
    testdataset1: Dataset,
):
    # testdataset1 has suggest_abbreviation_uppercase_org_single_token_shorter_than: 8
    org_data = {"id": "doe", "schema": "Organization", "properties": {}}
    org = Entity(testdataset1, org_data)

    # Short, all-uppercase, no space -> suggests abbreviation
    reg = check_name_regularity(org, "ABC")
    assert reg.is_irregular
    assert reg.suggested_prop == "abbreviation"

    # At or above threshold -> not caught by this heuristic
    reg = check_name_regularity(org, "ABCDEFGH")  # len 8, not < 8
    assert not reg.is_irregular

    # Has lowercase -> not caught by this heuristic (long enough to pass other checks)
    reg = check_name_regularity(org, "Abcde")
    assert not reg.is_irregular

    # Has space -> not caught by this heuristic (long enough to pass other checks)
    reg = check_name_regularity(org, "AB CD")
    assert not reg.is_irregular


def test_suggest_abbreviation_non_person_single_token_shorter_than(
    testdataset1: Dataset,
):
    # testdataset1 has suggest_abbreviation_non_person_single_token_shorter_than: 5
    # Plain LegalEntity (not Person, not Organization) is the primary target
    legal_data = {"id": "le", "schema": "LegalEntity", "properties": {}}
    legal = Entity(testdataset1, legal_data)
    person_data = {"id": "jon", "schema": "Person", "properties": {}}
    person = Entity(testdataset1, person_data)

    # Short, all-uppercase, no space LegalEntity -> suggests abbreviation
    reg = check_name_regularity(legal, "ABCD")
    assert reg.is_irregular
    assert reg.suggested_prop == "abbreviation"

    # Does NOT apply to Person
    reg = check_name_regularity(person, "ABCD")
    assert reg.is_irregular
    # caught by suggest_weak_alias_person_single_token
    assert reg.suggested_prop == "weakAlias"

    # At or above threshold -> not caught by this heuristic
    reg = check_name_regularity(legal, "ABCDE")  # len 5, not < 5
    assert not reg.is_irregular

    # Has lowercase -> not caught
    reg = check_name_regularity(legal, "Abcd")
    assert not reg.is_irregular
