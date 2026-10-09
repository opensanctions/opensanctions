"""Tests for validating the funes brief carried in zavod dataset
metadata."""

from typing import Any

import pytest
from pydantic import ValidationError

from zavod.meta import Dataset
from zavod.shed.funes.definitions import dataset_definition


def make_dataset(config: dict[str, Any]) -> Dataset:
    return Dataset({"name": "test_funes", "title": "Test", "config": config})


def brief(**overrides: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "people_sought": "board members",
        "subject_label": "Organization",
        "revisit_interval_days": 30,
        "subjects": [{"name": "Bank", "urls": ["https://bank.example/board"]}],
    }
    data.update(overrides)
    return data


def test_none_without_brief() -> None:
    assert dataset_definition(make_dataset({})) is None
    assert dataset_definition(make_dataset({"funes": None})) is None


def test_empty_brief_rejected() -> None:
    with pytest.raises(ValidationError):
        dataset_definition(make_dataset({"funes": {}}))


def test_empty_subjects_rejected() -> None:
    with pytest.raises(ValidationError):
        dataset_definition(make_dataset({"funes": brief(subjects=[])}))


def test_valid_brief() -> None:
    dataset = make_dataset({"funes": brief()})
    definition = dataset_definition(dataset)
    assert definition is not None
    assert definition.name == "test_funes"
    assert definition.people_sought == "board members"
    assert definition.subject_label == "Organization"
    assert definition.revisit_interval_days == 30
    assert definition.subjects[0].name == "Bank"
    assert definition.subjects[0].urls == ["https://bank.example/board"]


def test_revisit_interval_validated() -> None:
    for days in (0, -30):
        with pytest.raises(ValidationError):
            dataset_definition(
                make_dataset({"funes": brief(revisit_interval_days=days)})
            )


def test_missing_revisit_interval_rejected() -> None:
    data = brief()
    del data["revisit_interval_days"]
    with pytest.raises(ValidationError):
        dataset_definition(make_dataset({"funes": data}))


def test_noncanonical_urls_rejected() -> None:
    for url in (
        "#x",
        "not a url",
        "/relative",
        "mailto:a@b.example",
        "www.x.example/board",
    ):
        with pytest.raises(ValidationError):
            dataset_definition(
                make_dataset({"funes": brief(subjects=[{"name": "B", "urls": [url]}])})
            )


def test_extra_keys_rejected() -> None:
    with pytest.raises(ValidationError):
        dataset_definition(make_dataset({"funes": brief(x=1)}))
    with pytest.raises(ValidationError):
        dataset_definition(
            make_dataset({"funes": brief(subjects=[{"name": "B", "junk": True}])})
        )


def test_non_mapping_brief_rejected() -> None:
    with pytest.raises(ValueError, match="must be a mapping"):
        dataset_definition(make_dataset({"funes": ["nope"]}))


def test_urls_optional() -> None:
    dataset = make_dataset({"funes": brief(subjects=[{"name": "Bank"}])})
    definition = dataset_definition(dataset)
    assert definition is not None
    assert definition.subjects[0].urls == []
