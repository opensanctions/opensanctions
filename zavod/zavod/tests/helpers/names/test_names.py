import json
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import func, select
from structlog.testing import capture_logs

from zavod.context import Context
from zavod.extract.names.clean import Names, LangText
from zavod.stateful.model import review_table
from zavod.helpers import (
    apply_name,
    apply_reviewed_names,
    make_name,
    split_comma_names,
)
from zavod.helpers.names import apply_names, review_key_parts, review_names
from zavod.stateful.review import Review, review_key


def count_review_rows(conn, key: str) -> int:
    sel = (
        select(func.count()).select_from(review_table).where(review_table.c.key == key)
    )
    return conn.execute(sel).scalar()


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


@patch("zavod.helpers.names.settings.OPENAI_API_KEY", None)  # For validity
def test_apply_reviewed_names_no_cleaning_needed(vcontext: Context):
    """The original name is used."""

    entity = vcontext.make("Person")
    entity.id = "bla"
    original = Names(name="Jim Doe")
    apply_reviewed_names(vcontext, entity, original=original)
    assert entity.get("name") == ["Jim Doe"]
    assert entity.get("alias") == []
    key = review_key(review_key_parts(entity, original))
    assert count_review_rows(vcontext.conn, key) == 0


@patch("zavod.helpers.names.settings.OPENAI_API_KEY", None)
@patch("zavod.extract.names.clean.run_typed_text_prompt")
def test_apply_reviewed_names_llm_service_fallback(
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

    original = Names(name=raw_name)
    apply_reviewed_names(vcontext, entity, original=original, llm_cleaning=True)

    assert not run_typed_text_prompt.called, run_typed_text_prompt.call_args_list


@patch("zavod.helpers.names.settings.OPENAI_API_KEY", "AAABBBCCC")  # For validity
@patch("zavod.extract.names.clean.run_typed_text_prompt")
def test_apply_reviewed_names_llm(run_typed_text_prompt: MagicMock, vcontext: Context):
    """
    The original name is used.
    A review is created but the automatically extracted names are not applied until accepted.
    LLM-based cleaning is used.
    """

    entity = vcontext.make("Person")
    entity.id = "bla"
    raw_name = "Jim Doe; James Doe"

    run_typed_text_prompt.return_value = Names(name="James Doe", alias="Jim Doe")

    original = Names(name=raw_name)
    apply_reviewed_names(vcontext, entity, original=original, llm_cleaning=True)

    assert run_typed_text_prompt.called, run_typed_text_prompt.call_args_list

    # Until it's accepted, the original string is applied.
    assert entity.get("name") == [raw_name]
    assert entity.get("alias") == []
    entity.set("name", [])  # clear to test after accept

    # simulate accepting the review.
    names = Names(name=raw_name)
    key = review_key(review_key_parts(entity, names))

    review = Review.by_key(vcontext.conn, Names, vcontext.dataset.name, key)
    review.accepted = True
    review.save(vcontext.conn, new_revision=True)

    apply_reviewed_names(vcontext, entity, original=original, llm_cleaning=True)
    assert entity.get("name") == ["James Doe"]
    assert entity.get("alias") == ["Jim Doe"]


@patch("zavod.extract.names.clean.run_typed_text_prompt")
def test_apply_reviewed_names_manual_irregular(
    run_typed_text_prompt: MagicMock, vcontext: Context
):
    """
    A review is created but the manually extracted names are not applied until accepted.
    LLM-based cleaning is not used.
    """

    entity = vcontext.make("Person")
    entity.id = "bla"
    raw_name = "Jim Doe; James Doe"

    run_typed_text_prompt.return_value = Names(alias=["SHOULD NOT END UP IN ENTITY"])

    original = Names(name=raw_name)
    apply_reviewed_names(vcontext, entity, original=original)

    # LLM cleaning wasn't invoked
    assert not run_typed_text_prompt.called, run_typed_text_prompt.call_args_list

    # Original extraction is original names
    key = review_key(review_key_parts(entity, original))
    review = Review.by_key(vcontext.conn, Names, vcontext.dataset.name, key)
    assert review.extracted_data == original

    # Until it's accepted, the original string is applied.
    assert entity.get("name") == [raw_name]
    assert entity.get("alias") == []
    entity.set("name", [])  # clear to test after accept

    # simulate manually editing and accepting the review.
    review = Review.by_key(vcontext.conn, Names, vcontext.dataset.name, key)
    review.accepted = True
    review.extracted_data = Names(name=["James Doe"], alias=["Jim Doe"])
    review.save(vcontext.conn, new_revision=True)

    apply_reviewed_names(vcontext, entity, original=original)
    assert entity.get("name") == ["James Doe"]
    assert entity.get("alias") == ["Jim Doe"]


def test_apply_reviewed_names_suggested_with_llm_cleaning_raises(vcontext: Context):
    """
    Verify that when both suggested and llm_cleaning are provided,
    an AssertionError is raised.
    """
    entity = vcontext.make("Person")
    entity.id = "bla"

    original = Names(name="Jim Doe")
    suggested = Names(name="James Doe")

    with pytest.raises(AssertionError, match="LLM cleaning is enabled"):
        apply_reviewed_names(
            vcontext, entity, original=original, suggested=suggested, llm_cleaning=True
        )


def test_apply_reviewed_names_suggested_no_llm(vcontext: Context):
    """
    A review is created if suggested different from original is passed.
    Neither original nor suggested needs to be irregular.

    original names are applied until it's accepted.
    """

    entity = vcontext.make("Person")
    entity.id = "bla"
    raw_name = "Jim Doe"  # Not irregular

    original = Names(name=raw_name)
    suggested = Names(alias=raw_name)
    apply_reviewed_names(vcontext, entity, original=original, suggested=suggested)

    # Original extraction is suggested names
    key = review_key(review_key_parts(entity, original))
    review = Review.by_key(vcontext.conn, Names, vcontext.dataset.name, key)
    assert review.extracted_data == suggested

    # Source value is based on original without blanks and, not suggested.
    assert review.source_value == json.dumps(
        {"entity_schema": "Person", "original": {"name": ["Jim Doe"]}}, indent=2
    )

    # Until it's accepted, the suggestions are not applied.
    assert entity.get("name") == [raw_name]
    assert entity.get("alias") == []


def test_apply_reviewed_names_suggested_original(vcontext: Context):
    """
    If suggested equals original, no review is created unless is_irregular is True.

    Test that the crawler can have its own notion of irregular even if
    it doesn't suggest a re-categorisation.
    """

    entity = vcontext.make("Person")
    entity.id = "bla"
    raw_name = "Jim Doe"  # Not irregular

    original = Names(name=raw_name)
    suggested = Names(name=raw_name)
    review_names(vcontext, entity, original=original, suggested=suggested)
    key = review_key(review_key_parts(entity, original))
    assert count_review_rows(vcontext.conn, key) == 0

    review_names(
        vcontext, entity, original=original, suggested=suggested, is_irregular=True
    )
    assert count_review_rows(vcontext.conn, key) == 1


def test_apply_names_with_lang_argument(vcontext: Context):
    """When lang argument is supplied, it should be used for str values."""
    entity = vcontext.make("Person")
    entity.id = "test"

    original = Names(name="John Doe")
    names = Names(name="John Doe", alias="Johnny")

    apply_names(entity, original=original, names=names, lang="eng")

    # Check that lang is applied to all str values
    for stmt in entity.get_statements("name"):
        assert stmt.lang == "eng"
    for stmt in entity.get_statements("alias"):
        assert stmt.lang == "eng"


def test_apply_names_with_langtext(vcontext: Context):
    """When value is LangText, its lang should be used, overriding the lang argument."""
    entity = vcontext.make("Person")
    entity.id = "test"

    original = Names(name="John Doe")
    names = Names(
        name=LangText(text="John Doe", lang="eng"),
        alias=LangText(text="جون دو", lang="ara"),
    )

    # Even though we pass lang="fr", LangText's own lang should be used
    apply_names(entity, original=original, names=names, lang="fra")

    # Check that LangText's lang is used
    name_stmts = list(entity.get_statements("name"))
    assert len(name_stmts) == 1
    assert name_stmts[0].value == "John Doe"
    assert name_stmts[0].lang == "eng"

    alias_stmts = list(entity.get_statements("alias"))
    assert len(alias_stmts) == 1
    assert alias_stmts[0].value == "جون دو"
    assert alias_stmts[0].lang == "ara"


def test_apply_names_langtext_with_none_lang(vcontext: Context):
    """When LangText has lang=None, None should be used even if lang argument is supplied."""
    entity = vcontext.make("Person")
    entity.id = "test"

    original = Names(name="John Doe")
    names = Names(
        name=LangText(text="John Doe", lang=None),
    )

    # Even though we pass lang="eng", LangText's None should override it
    apply_names(entity, original=original, names=names, lang="eng")

    # Check that lang is None
    name_stmts = list(entity.get_statements("name"))
    assert len(name_stmts) == 1
    assert name_stmts[0].value == "John Doe"
    assert name_stmts[0].lang is None
