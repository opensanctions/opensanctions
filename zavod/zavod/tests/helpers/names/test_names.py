from unittest.mock import MagicMock, patch

from structlog.testing import capture_logs

from zavod.context import Context
from zavod.entity import Entity
from zavod.extract.names.clean import Names
from zavod.helpers import (
    apply_name,
    apply_reviewed_name_string,
    make_name,
    split_comma_names,
    is_name_irregular,
)
from zavod.helpers.names import review_key_parts
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


@patch("zavod.helpers.names.settings.OPENAI_API_KEY", None)  # For validity
def test_apply_reviewed_name_string_no_cleaning_needed(vcontext: Context):
    """The original name is used."""

    entity = vcontext.make("Person")
    entity.id = "bla"
    apply_reviewed_name_string(vcontext, entity, string="Jim Doe")
    assert entity.get("name") == ["Jim Doe"]
    assert entity.get("alias") == []
    entity.set("name", [])  # clear to test alias

    apply_reviewed_name_string(
        vcontext, entity, string="Jim Doe", original_prop="alias"
    )
    assert entity.get("name") == []
    assert entity.get("alias") == ["Jim Doe"]


@patch("zavod.helpers.names.settings.OPENAI_API_KEY", None)
@patch("zavod.extract.names.clean.run_typed_text_prompt")
def test_apply_reviewed_name_string_ci_fallback(
    run_typed_text_prompt: MagicMock, vcontext: Context
):
    """
    Verify that when env var OPENAI_API_KEY is set, we don't call OpenAPI,
    and we fall back to original name.
    """
    entity = vcontext.make("Person")
    entity.id = "bla"
    raw_name = "Jim Doe; James Doe"

    run_typed_text_prompt.return_value = Names(name=["Jim Doe", "James Doe"])

    apply_reviewed_name_string(
        vcontext, entity, string=raw_name, enable_llm_cleaning=True
    )

    assert not run_typed_text_prompt.called, run_typed_text_prompt.call_args_list


@patch("zavod.helpers.names.settings.OPENAI_API_KEY", "AAABBBCCC")  # For validity
@patch("zavod.extract.names.clean.run_typed_text_prompt")
def test_apply_reviewed_name_string_llm(
    run_typed_text_prompt: MagicMock, vcontext: Context
):
    """
    The original name is used.
    A review is created but the automatically extracted names are not applied until accepted.
    LLM-based cleaning is used.
    """

    entity = vcontext.make("Person")
    entity.id = "bla"
    raw_name = "Jim Doe; James Doe"

    run_typed_text_prompt.return_value = Names(name="James Doe", alias="Jim Doe")

    apply_reviewed_name_string(
        vcontext, entity, string=raw_name, enable_llm_cleaning=True
    )

    assert run_typed_text_prompt.called, run_typed_text_prompt.call_args_list

    # Until it's accepted, the original string is applied.
    assert entity.get("name") == [raw_name]
    assert entity.get("alias") == []
    entity.set("name", [])  # clear to test after accept

    # simulate accepting the review.
    names = Names(name=[raw_name])
    key = review_key(review_key_parts(entity, names))

    review = Review.by_key(vcontext.conn, Names, vcontext.dataset.name, key)
    review.accepted = True
    review.save(vcontext.conn, new_revision=True)

    apply_reviewed_name_string(
        vcontext, entity, string=raw_name, enable_llm_cleaning=True
    )
    assert entity.get("name") == ["James Doe"]
    assert entity.get("alias") == ["Jim Doe"]


@patch("zavod.helpers.names.settings.OPENAI_API_KEY", "AAABBBCCC")
@patch("zavod.extract.names.clean.run_typed_text_prompt")
def test_apply_reviewed_name_string_manual(
    run_typed_text_prompt: MagicMock, vcontext: Context
):
    """
    The original name is used.
    A review is created but the manually extracted names are not applied until accepted.
    LLM-based cleaning is not used.
    """

    entity = vcontext.make("Person")
    entity.id = "bla"
    raw_name = "Jim Doe; James Doe"

    run_typed_text_prompt.return_value = Names(alias=["SHOULD NOT END UP IN ENTITY"])

    apply_reviewed_name_string(vcontext, entity, string=raw_name)

    assert not run_typed_text_prompt.called, run_typed_text_prompt.call_args_list

    # Until it's accepted, the original string is applied.
    assert entity.get("name") == [raw_name]
    assert entity.get("alias") == []
    entity.set("name", [])  # clear to test after accept

    # simulate manually editing and accepting the review.
    names = Names(name=[raw_name])
    key = review_key(review_key_parts(entity, names))

    review = Review.by_key(vcontext.conn, Names, vcontext.dataset.name, key)
    review.accepted = True
    review.extracted_data = Names(name=["James Doe"], alias=["Jim Doe"])
    review.save(vcontext.conn, new_revision=True)

    apply_reviewed_name_string(vcontext, entity, string=raw_name)
    assert entity.get("name") == ["James Doe"]
    assert entity.get("alias") == ["Jim Doe"]
