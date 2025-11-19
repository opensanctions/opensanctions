from unittest.mock import MagicMock, patch

from structlog.testing import capture_logs

from zavod.context import Context
from zavod.entity import Entity
from zavod.extract.names.clean import CleanNames
from zavod.helpers import (
    apply_name,
    apply_reviewed_names,
    make_name,
    is_name_irregular,
    split_comma_names,
)
from zavod.meta.dataset import Dataset
from zavod.stateful.review import Review, review_key


def test_make_name():
    name = make_name(first_name="John", last_name="Doe")
    assert name == "John Doe"


def test_entity_name(vcontext: Context):
    entity = vcontext.make("Person")
    entity.id = "bla"
    apply_name(
        entity,
        first_name="John",
        second_name="Brandon",
        last_name="Doe",
        lang="eng",
    )
    assert "John Brandon Doe" in entity.get("name")
    assert entity.caption == "John Brandon Doe"
    for stmt in entity.get_statements("name"):
        assert stmt.lang == "eng"


def test_full_name(vcontext: Context):
    entity = vcontext.make("Person")
    entity.id = "bla"
    apply_name(
        entity,
        full="Zorro",
        first_name="John",
        second_name="Brandon",
        last_name="Doe",
        lang="eng",
    )
    assert entity.caption == "Zorro"
    for stmt in entity.get_statements("name"):
        assert stmt.lang == "eng"


def test_alias_name(vcontext: Context):
    entity = vcontext.make("Person")
    entity.id = "bla"
    apply_name(
        entity,
        first_name="John",
        second_name="Brandon",
        last_name="Doe",
        lang="eng",
        alias=True,
    )
    assert entity.get("name") == []
    assert "John Brandon Doe" in entity.get("alias")
    apply_name(
        entity,
        first_name="Johnny",
        last_name="Doe",
        lang="eng",
        is_weak=True,
    )
    assert "Johnny Doe" in entity.get("weakAlias")


def test_company_name(vcontext: Context):
    entity = vcontext.make("Company")
    entity.id = "bla"
    apply_name(
        entity,
        first_name="Hansen",
        last_name="Enterprises",
        lang="eng",
        alias=True,
        quiet=True,
    )
    assert entity.get("name") == []
    assert "Hansen Enterprises" in entity.get("alias")
    assert entity.get("firstName", quiet=True) == []


def test_split_comma_names(vcontext: Context, caplog):
    assert split_comma_names(vcontext, "") == []
    assert split_comma_names(vcontext, "John Smith") == ["John Smith"]
    assert split_comma_names(vcontext, "Smith, John") == ["Smith, John"]
    assert split_comma_names(vcontext, "John Smith, Jr.") == ["John Smith Jr."]
    assert split_comma_names(vcontext, "A B C, D E F") == ["A B C", "D E F"]
    assert split_comma_names(vcontext, "A B C, Ltd., D E F, Inc.") == [
        "A B C Ltd.",
        "D E F Inc.",
    ]
    assert split_comma_names(vcontext, "A B and C, D E F, John Lookups Smith") == [
        "A B and C",
        "D E F",
        "John Lookups Smith",
    ]

    # Not in lookups

    # Shouldn't be split
    assert split_comma_names(vcontext, "A, B and C Ltd.") == ["A, B and C Ltd."]
    # Would have been nice if this could be split
    assert split_comma_names(
        vcontext, "songyan li, junhong xiong, k. ivan gothner and edward pazdro"
    ) == ["songyan li, junhong xiong, k. ivan gothner and edward pazdro"]

    with capture_logs() as cap_logs:
        # Would have been nice if this could be split
        assert split_comma_names(vcontext, "A B and C, D E F") == ["A B and C, D E F"]
    logs = [f"{entry['log_level']}: {entry['event']}" for entry in cap_logs]
    assert "warning: Not sure how to split on comma or and." in logs

    # We cannot not decide in a case like A and B Ltd. It can be the name of one company or two seperate entities
    with capture_logs() as cap_logs:
        assert split_comma_names(vcontext, "A and B Ltd.") == ["A and B Ltd."]
    logs = [f"{entry['log_level']}: {entry['event']}" for entry in cap_logs]
    assert "warning: Not sure how to split on comma or and." in logs


def test_is_name_irregular(testdataset1: Dataset):
    org_data = {"id": "doe", "schema": "Organization", "properties": {}}
    org = Entity(testdataset1, org_data)
    assert not is_name_irregular(org, "Company Ltd.")
    # Default
    assert is_name_irregular(org, "Company Ltd; Holding Company Ltd.")
    # Extra
    assert is_name_irregular(org, "Company Ltd, Holding Company Ltd.")


@patch("zavod.helpers.names.settings.CI", False)  # For validity
def test_apply_reviewed_names_no_cleaning_needed(vcontext: Context):
    """The original name is used."""

    entity = vcontext.make("Person")
    entity.id = "bla"
    apply_reviewed_names(vcontext, entity, "Jim Doe")
    assert entity.get("name") == ["Jim Doe"]
    assert entity.get("alias") == []
    entity.set("name", [])  # clear to test alias

    apply_reviewed_names(vcontext, entity, "Jim Doe", alias=True)
    assert entity.get("name") == []
    assert entity.get("alias") == ["Jim Doe"]


@patch("zavod.helpers.names.settings.CI", True)
@patch("zavod.extract.names.clean.run_typed_text_prompt")
def test_apply_reviewed_names_ci_fallback(
    run_typed_text_prompt: MagicMock, vcontext: Context
):
    """
    Verify that when env var CI is set, we fall back to original name.
    Mocking to verify that outside CI (on our laptops)
    """
    entity = vcontext.make("Person")
    entity.id = "bla"
    raw_name = "Jim Doe; James Doe"

    run_typed_text_prompt.return_value = CleanNames(
        full_name=["Jim Doe", "James Doe"],
        alias=[],
        weak_alias=[],
        previous_name=[],
    )

    apply_reviewed_names(vcontext, entity, raw_name)

    assert not run_typed_text_prompt.called, run_typed_text_prompt.call_args_list


@patch("zavod.helpers.names.settings.CI", False)
@patch("zavod.extract.names.clean.run_typed_text_prompt")
def test_apply_reviewed_names(run_typed_text_prompt: MagicMock, vcontext: Context):
    """
    The original name is used.
    A review is created but the unaccepted name(s) are not applied.
    """

    entity = vcontext.make("Person")
    entity.id = "bla"
    raw_name = "Jim Doe; James Doe"

    run_typed_text_prompt.return_value = CleanNames(
        full_name=["Jim Doe", "James Doe"],
        alias=[],
        weak_alias=[],
        previous_name=[],
    )

    apply_reviewed_names(vcontext, entity, raw_name)

    assert run_typed_text_prompt.called, run_typed_text_prompt.call_args_list

    # Until it's accepted, the original string is applied.
    assert entity.get("name") == [raw_name]
    assert entity.get("alias") == []
    entity.set("name", [])  # clear to test alias
    apply_reviewed_names(vcontext, entity, raw_name, alias=True)
    assert entity.get("alias") == [raw_name]
    assert entity.get("name") == []
    entity.set("alias", [])  # clear to test after accept

    # simulate accepting the review.
    key = review_key(raw_name)
    review = Review.by_key(vcontext.conn, CleanNames, vcontext.dataset.name, key)
    review.accepted = True
    review.save(vcontext.conn, new_revision=True)

    apply_reviewed_names(vcontext, entity, raw_name)
    assert set(entity.get("name")) == {"Jim Doe", "James Doe"}
    assert entity.get("alias") == []
    entity.set("name", [])  # clear to test alias

    apply_reviewed_names(vcontext, entity, raw_name, alias=True)
    assert entity.get("name") == []
    assert set(entity.get("alias")) == {"Jim Doe", "James Doe"}
