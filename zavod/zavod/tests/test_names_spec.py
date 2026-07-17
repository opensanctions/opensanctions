import pytest
from pydantic import ValidationError

from zavod.meta.names import NamesSpec


def test_typo_in_default_schema_override_raises():
    # Person is one of the default schemata: the override merges with the
    # default spec but must still go through full pydantic validation.
    with pytest.raises(ValidationError):
        NamesSpec.model_validate(
            {"schema_rules": {"Person": {"rejct_strings": ["and"]}}}
        )


def test_wrong_type_in_default_schema_override_raises():
    with pytest.raises(ValidationError):
        NamesSpec.model_validate(
            {"schema_rules": {"Person": {"reject_chars": [",", ";"]}}}
        )


def test_typo_in_new_schema_raises():
    with pytest.raises(ValidationError):
        NamesSpec.model_validate(
            {"schema_rules": {"Organization": {"rejct_strings": ["and"]}}}
        )


def test_default_schema_override_merges_with_defaults():
    spec = NamesSpec.model_validate(
        {"schema_rules": {"Person": {"reject_strings": [" and "]}}}
    )
    person = spec.schema_rules["Person"]
    # The override is applied
    assert person.reject_strings == [" and "]
    # Default values are retained
    assert ";" in person.reject_chars_baseline
    assert person.require_space is True
    assert ";" in person.reject_chars_consolidated
    # Other default schemata are untouched
    assert "Vessel" in spec.schema_rules


def test_input_dict_is_not_mutated():
    obj = {"schema_rules": {"Person": {"reject_strings": [" and "]}}}
    first = NamesSpec.model_validate(obj)
    # The input dict still carries the rules after validation
    assert obj == {"schema_rules": {"Person": {"reject_strings": [" and "]}}}
    second = NamesSpec.model_validate(obj)
    assert first.schema_rules == second.schema_rules
    assert second.schema_rules["Person"].reject_strings == [" and "]
