from functools import cache
from pathlib import Path
from typing import Any
from collections.abc import Generator, Sequence
import json

from pydantic import BaseModel
from zavod.context import Context
from zavod.extract.llm import run_typed_text_prompt

SINGLE_ENTITY_PROGRAM_PATH = Path(__file__).parent / "single_entity_program.json"
# Properties that shouldn't be shown to the reviewer if they are empty,
# so that they aren't tempted into populating them unless they had a value in the
# original extraction.
EXCLUDE_IF_EMPTY = {"previousName", "firstName", "middleName", "lastName"}


class LangText(BaseModel):
    text: str
    lang: str | None
    """ISO 639-2 (3-letter) language code, or None if not known"""

    def __hash__(self) -> int:
        return hash((self.text, self.lang))


# A fairly broad set of types to reduce boilerplate editing in reviews.
# See SimplifiedNames and LangNames for more specific types for specific use cases.
NamesValues = None | str | Sequence[str | LangText]


class Names(BaseModel):
    """
    Names of a single entity.

    This is used both to represent how strings containing one or more names have been
    extracted from source data, as categorised by the source, and also to capture a
    proposed and eventually analyst-reviewed and accepted cleaned version of those names.

    Cleaning might include splitting or re-combining parts, and stripping punctuation
    which does not form part of the name.

    Unknown keys are deliberately tolerated on validation (pydantic's default
    ``extra="ignore"``): stored review payloads are re-validated with the current
    model, and reviewer-edited or legacy payloads can carry keys that are not
    (or no longer) fields. Construction sites that build a Names from dynamic
    keys must validate them against ``Names.model_fields`` instead.
    """

    name: NamesValues = None
    alias: NamesValues = None
    weakAlias: NamesValues = None
    previousName: NamesValues = None
    abbreviation: NamesValues = None
    # TODO: Before adding name parts, we should consider whether we should
    # add them directly or construct a full name with them via h.apply_name.
    #
    # Also make sure the LLM and users aren't tempted to infer them.
    #
    # firstName: NamesValue = None
    # middleName: NamesValue = None
    # lastName: NamesValue = None

    def _is_blank_value(self, value: NamesValues) -> bool:
        """Check if a value is blank (None, empty string, or empty list)."""
        if value is None:
            return True
        if isinstance(value, str):
            return is_empty_string(value)
        if isinstance(value, list):
            return len(value) == 0 or all(is_empty_string(v) for v in value)
        return False

    def model_dump(self, **kwargs: Any) -> dict[str, Any]:
        result = super().model_dump(**kwargs)
        # Leave out uncommon fields when dumping if they are empty
        # to keep the extracted value in Data Reviews simpler for review.
        return {
            key: value
            for key, value in result.items()
            if key not in EXCLUDE_IF_EMPTY or not self._is_blank_value(value)
        }

    def is_empty(self) -> bool:
        for prop, names in self.as_langtexts():
            return False
        return True

    def as_langtexts(self) -> Generator[tuple[str, list[LangText]], None, None]:
        """
        Generator yielding each property and a list of any associated non-empty name values.

        Plain str values are wrapped as LangText with lang=None.

        Useful when iterating over values in a Names instance.
        """
        for key in self.__class__.model_fields:
            value = getattr(self, key)
            if value is None:
                continue
            if isinstance(value, (str, LangText)):
                if not is_empty_string(value):
                    yield key, [_to_lang_text(value)]
            elif isinstance(value, list):
                nonempty_values = [
                    _to_lang_text(v) for v in value if not is_empty_string(v)
                ]
                if nonempty_values:
                    yield key, nonempty_values

    def add(self, prop: str, value: str | None, *, lang: str | None = None) -> None:
        """
        Add a value to a property. If set as a single value, the values are added to a list.
        Value is wrapped in LangText if lang is provided.

        Note: Names with LangText language values and llm_cleaning=True are not supported together.
        If names share the same language, pass ``lang`` to ``apply_reviewed_names`` /
        ``apply_reviewed_name_string`` instead. If names have different languages, use a
        separate call per language.

        Args:
            prop: The property name to add the value to.
            value: The name value to add.
            lang: Optional ISO 639-2 language code for the name value.
        """

        if value is None:
            return
        item = LangText(text=value, lang=lang) if lang is not None else value
        current = getattr(self, prop)
        if current is None:
            setattr(self, prop, item)
        elif isinstance(current, list):
            current.append(item)
        else:
            setattr(self, prop, [current, item])

    def simplified(self) -> "Names":
        """Get a copy where single-item lists are replaced by just the single item,
        and LangText values with lang=None are simplified to plain strings.
        This is useful for formatting for human editing in reviews."""

        def simplify_val(v: str | LangText) -> str | LangText:
            if isinstance(v, LangText) and v.lang is None:
                return v.text
            return v

        data: dict[str, str | list[str | LangText] | None] = {}
        for key in self.__class__.model_fields:
            value = getattr(self, key)
            if isinstance(value, (str, LangText)):
                sv = simplify_val(value)
                wrapped: list[str | LangText] = [sv]
                data[key] = sv if isinstance(sv, str) else wrapped
            elif isinstance(value, list):
                simplified = [simplify_val(v) for v in value]
                if not simplified:
                    data[key] = None
                elif len(simplified) == 1 and isinstance(simplified[0], str):
                    data[key] = simplified[0]
                else:
                    data[key] = simplified
            else:
                data[key] = value
        return Names(**data)

    def __eq__(self, value: object) -> bool:
        assert isinstance(value, Names), type(value)

        # we don't care about value order within a prop
        # we don't care about value repetition within a prop
        # we do care about value repetition across props
        # single values and single-item lists are considered equal
        # str and LangText(text=str, lang=None) are considered equal
        def to_dict(names: "Names") -> dict[str, frozenset[LangText]]:
            return {prop: frozenset(vals) for prop, vals in names.as_langtexts()}

        return to_dict(self) == to_dict(value)


class SimpleNames(Names):
    """Simplified type options to keep potential output format for LLMs simpler."""

    name: Sequence[str] = []
    alias: Sequence[str] = []
    weakAlias: Sequence[str] = []
    previousName: Sequence[str] = []
    abbreviation: Sequence[str] = []


class LangNames(Names):
    """Simplified Names where all values are LangText to make processing simpler."""

    name: Sequence[LangText] = []
    alias: Sequence[LangText] = []
    weakAlias: Sequence[LangText] = []
    previousName: Sequence[LangText] = []
    abbreviation: Sequence[LangText] = []


class SourceNames(BaseModel):
    """Name strings and schema supplied to the LLM for cleaning and categorisation"""

    entity_schema: str
    original: Names


class SingleEntityInput(BaseModel):
    """The flattened input sent to the single-entity prompt as JSON."""

    entity_schema: str
    strings: list[str]


class DSPySignature(BaseModel):
    instructions: str


class PredictProgramData(BaseModel):
    """The name cleaning program artifact written by ``contrib/tune``.

    Besides the serialised DSPy program (of which only the prompt instructions
    are used here), the artifact carries a plain contract: which model the
    prompt was tuned for, and which input and output fields it expects.
    """

    model: str
    input_fields: list[str]
    output_fields: list[str]
    signature: DSPySignature


def _to_lang_text(value: str | LangText) -> LangText:
    if isinstance(value, str):
        return LangText(text=value, lang=None)
    return value


def is_empty_string(text: str | LangText | None) -> bool:
    if text is None:
        return True
    if isinstance(text, LangText):
        return is_empty_string(text.text)
    if isinstance(text, str):
        text = text.strip()
        return len(text) == 0
    return False


@cache
def load_single_entity_program() -> PredictProgramData:
    """Load the tuned program artifact, validating its field contract.

    The expected fields are derived from the models in this module: the
    prompt takes ``SingleEntityInput`` and produces every ``SimpleNames``
    field except ``abbreviation``, which is analyst-only and never
    requested from the LLM.
    """
    with open(SINGLE_ENTITY_PROGRAM_PATH) as program_file:
        program = PredictProgramData.model_validate_json(program_file.read())
    expected_input = list(SingleEntityInput.model_fields)
    expected_output = [
        field for field in SimpleNames.model_fields if field != "abbreviation"
    ]
    if sorted(program.input_fields) != sorted(expected_input):
        raise ValueError(
            f"Program artifact {SINGLE_ENTITY_PROGRAM_PATH} declares input "
            f"fields {program.input_fields}, expected {expected_input}. "
            "Regenerate the artifact with contrib/tune "
            "(see zavod/docs/extract/names.md)."
        )
    if sorted(program.output_fields) != sorted(expected_output):
        raise ValueError(
            f"Program artifact {SINGLE_ENTITY_PROGRAM_PATH} declares output "
            f"fields {program.output_fields}, expected {expected_output}. "
            "Regenerate the artifact with contrib/tune "
            "(see zavod/docs/extract/names.md)."
        )
    return program


def load_single_entity_prompt() -> str:
    """Load the prompt instructions from the tuned program artifact."""
    return load_single_entity_program().signature.instructions


def clean_names(context: Context, raw_names: SourceNames) -> SimpleNames:
    """Use an LLM to clean and categorise names."""
    program = load_single_entity_program()
    prompt = program.signature.instructions

    strings: list[str] = []
    for _prop, names in raw_names.original.as_langtexts():
        for name in names:
            if name.text not in strings:
                strings.append(name.text)

    input_data = SingleEntityInput(
        entity_schema=raw_names.entity_schema, strings=strings
    )
    # The prompt instructions frame the JSON input; only the serialized
    # input is sent alongside them.
    # ensure_ascii=False so that non-ASCII like Алтайкапиталбанк
    # doesn't get escaped like \u0410\u043b\u0442\u0430\u0439\u043a\u0430\u...
    # which then results in a name in the response like \x041\x041\x041 \x041\x041 \x041
    # probably because gpt4o isn't trained on escape sequences, and we only
    # need it to be JSON-ish embedded in the input string to give it some structure.
    input_string = json.dumps(input_data.model_dump(), indent=2, ensure_ascii=False)

    return run_typed_text_prompt(
        context=context,
        prompt=prompt,
        string=input_string,
        response_type=SimpleNames,
        model=program.model,
    )
