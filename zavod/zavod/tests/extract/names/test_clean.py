import json

import pytest

from zavod.extract.names.clean import (
    LangText,
    Names,
    SINGLE_ENTITY_PROGRAM_PATH,
    load_single_entity_program,
    load_single_entity_prompt,
)


def test_lang_text_equality():
    assert LangText(text="John Doe", lang="eng") == LangText(
        text="John Doe", lang="eng"
    )
    assert LangText(text="John Doe", lang=None) == LangText(text="John Doe", lang=None)
    assert LangText(text="John Doe", lang="eng") != LangText(text="John Doe", lang=None)
    assert LangText(text="John Doe", lang="eng") != LangText(
        text="Jane Doe", lang="eng"
    )
    assert LangText(text="John Doe", lang="eng") != LangText(
        text="John Doe", lang="fra"
    )


def test_names_equality():
    assert Names() == Names()
    assert Names(name="John Doe") == Names(name="John Doe")
    assert Names(name="John Doe") != Names(name="Jane Doe")
    assert Names(name="John Doe") == Names(name=["John Doe"])
    assert Names(name="John Doe") == Names(name=[LangText(text="John Doe", lang=None)])
    assert Names(name="John Doe") != Names(name=[LangText(text="John Doe", lang="eng")])
    assert Names(name=["A", "B"]) == Names(name=["A", "B"])
    assert Names(name=["A", "B"]) == Names(name=["B", "A"])  # order doesn't matter
    assert Names(name=["A"]) != Names(alias=["A"])
    assert Names(name=["A"]) != Names(alias=["A"])
    assert Names(name=["A"]) != Names()


def test_names_simplified():
    # Single-item list is unwrapped
    assert Names(name=["John Doe"]).simplified().name == "John Doe"

    # Multi-item list stays as list
    assert Names(name=["John Doe", "Jane Doe"]).simplified().name == [
        "John Doe",
        "Jane Doe",
    ]

    # Single-item list of LangText with lang=None is unwrapped and simplified to str
    names = Names(name=[LangText(text="John Doe", lang=None)])
    assert names.simplified().name == "John Doe"

    # Single-item list of LangText with lang set stays as a list (can't be a bare LangText scalar)
    names = Names(name=[LangText(text="John Doe", lang="eng")])
    assert set(names.simplified().name) == {LangText(text="John Doe", lang="eng")}

    # Mixed list: lang=None items become strs, lang-set items stay as LangText
    names = Names(
        name=[
            LangText(text="John Doe", lang=None),
            LangText(text="جون دو", lang="ara"),
        ]
    )
    assert set(names.simplified().name) == {
        "John Doe",
        LangText(text="جون دو", lang="ara"),
    }

    # Empty list simplifies to None
    assert Names(name=[]).simplified().name is None


def test_names_tolerates_unknown_keys_on_validation():
    """Stored review payloads are re-validated with the current model, and
    reviewer-edited or legacy payloads can carry keys that are not (or no
    longer) fields — loading them must not raise. Fail-loud protection against
    typo'd keys lives at the dynamic construction sites instead, e.g.
    apply_reviewed_name_string."""
    # A reviewer-edited payload with an unknown key.
    names = Names.model_validate({"name": ["John Doe"], "fullName": ["J. Doe"]})
    assert names.name == ["John Doe"]

    # A legacy payload keyed with fields that no longer exist on the model.
    names = Names.model_validate({"name": ["John Doe"], "firstName": "John"})
    assert names.name == ["John Doe"]

    # A stored-review-shaped dump round-trips.
    names = Names(name=["John Doe"], alias="Johnny")
    assert Names.model_validate(names.model_dump()) == names


def test_load_single_entity_prompt():
    # The optimised program JSON must ship with the package: production name
    # cleaning reads its prompt from there without importing dspy.
    prompt = load_single_entity_prompt()
    assert isinstance(prompt, str)
    assert len(prompt.strip()) > 0


def test_load_single_entity_program_contract():
    # The artifact declares the model the prompt was tuned for and the fields
    # it expects; production reads both instead of keeping its own constants.
    program = load_single_entity_program()
    assert program.model == "gpt-5.4"
    assert sorted(program.input_fields) == ["entity_schema", "strings"]
    assert sorted(program.output_fields) == [
        "alias",
        "name",
        "previousName",
        "weakAlias",
    ]


def write_program(tmp_path, **overrides):
    artifact = json.loads(SINGLE_ENTITY_PROGRAM_PATH.read_text())
    artifact.update(overrides)
    path = tmp_path / "program.json"
    path.write_text(json.dumps(artifact))
    return path


def test_rejects_drifted_output_fields(tmp_path, monkeypatch):
    # If the tuning tooling ships a prompt working with different fields than
    # this module expects, loading must fail loudly rather than mismapping.
    path = write_program(tmp_path, output_fields=["name", "alias"])
    monkeypatch.setattr("zavod.extract.names.clean.SINGLE_ENTITY_PROGRAM_PATH", path)
    load_single_entity_program.cache_clear()
    try:
        with pytest.raises(ValueError, match="output fields"):
            load_single_entity_program()
    finally:
        load_single_entity_program.cache_clear()


def test_rejects_drifted_input_fields(tmp_path, monkeypatch):
    path = write_program(tmp_path, input_fields=["entity_schema", "names"])
    monkeypatch.setattr("zavod.extract.names.clean.SINGLE_ENTITY_PROGRAM_PATH", path)
    load_single_entity_program.cache_clear()
    try:
        with pytest.raises(ValueError, match="input fields"):
            load_single_entity_program()
    finally:
        load_single_entity_program.cache_clear()


def test_rejects_artifact_without_contract(tmp_path, monkeypatch):
    # Artifacts written before the contract keys existed must not load: there
    # would be no model version or field declaration to validate against.
    artifact = json.loads(SINGLE_ENTITY_PROGRAM_PATH.read_text())
    for key in ("model", "input_fields", "output_fields"):
        del artifact[key]
    path = tmp_path / "program.json"
    path.write_text(json.dumps(artifact))
    monkeypatch.setattr("zavod.extract.names.clean.SINGLE_ENTITY_PROGRAM_PATH", path)
    load_single_entity_program.cache_clear()
    try:
        with pytest.raises(ValueError):
            load_single_entity_program()
    finally:
        load_single_entity_program.cache_clear()
