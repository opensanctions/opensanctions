from functools import cache
from pathlib import Path
from typing import Any, Generator, List, Optional, Sequence, Tuple
import json

from pydantic import BaseModel
from zavod.context import Context
from zavod.extract.llm import run_typed_text_prompt

LLM_MODEL_VERSION = "gpt-4o"
SINGLE_ENTITY_PROGRAM_PATH = Path(__file__).parent / "dspy/single_entity_program.json"
# Properties that shouldn't be shown to the reviewer if they are empty,
# so that they aren't tempted into populating them unless they had a value in the
# original extraction.
EXCLUDE_IF_EMPTY = {"previousName", "firstName", "middleName", "lastName"}


class LangText(BaseModel):
    text: str
    lang: str
    """ISO 639-2 (3-letter) language code"""

    def __hash__(self) -> int:
        return hash((self.text, self.lang))


NamesValue = str | LangText | None
NamesValues = NamesValue | Sequence[NamesValue]


class Names(BaseModel):
    """Names categorised and cleaned of non-name characters."""

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
        return {
            key: value
            for key, value in result.items()
            if key not in EXCLUDE_IF_EMPTY or not self._is_blank_value(value)
        }

    def is_empty(self) -> bool:
        for prop, names in self.nonempty_item_lists():
            return False
        return True

    def nonempty_item_lists(
        self,
    ) -> Generator[Tuple[str, List[str | LangText]], None, None]:
        """
        Generator yielding each property and a list of any associated non-empty name values.

        Useful when iterating over values in a Names instance.
        """
        for key in self.__class__.model_fields:
            value = getattr(self, key)
            if value is None:
                continue
            if isinstance(value, (str, LangText)):
                if not is_empty_string(value):
                    yield key, [value]
            elif isinstance(value, list):
                nonempty_values = [v for v in value if not is_empty_string(v)]
                if nonempty_values:
                    yield key, nonempty_values

    def add(
        self, prop: str, value: Optional[str], *, lang: Optional[str] = None
    ) -> None:
        """
        Add a value to a property. If set as a single value, the values are added to a list.
        Value is wrapped in LangText if lang is provided.

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

    def simplify(self) -> "Names":
        """Get a copy where single-item lists are replaced by just the single item.
        This is useful for formatting for human editing in reviews."""
        data = {}

        for key, value in self.model_copy(deep=True).model_dump().items():
            if isinstance(value, list) and len(value) == 1:
                data[key] = value[0]
            else:
                data[key] = value
        return Names(**data)

    def __eq__(self, value: object) -> bool:
        assert isinstance(value, Names), type(value)

        # we care about prop order
        # we don't care about value order
        # we don't care about value repetition within a prop
        # we do care about value repetition across props
        # single values and single-item lists are considered equal
        for prop in self.__class__.model_fields:
            self_values = getattr(self, prop)
            other_values = getattr(value, prop)
            self_values_set = (
                set(self_values) if isinstance(self_values, list) else {self_values}
            )
            other_values_set = (
                set(other_values) if isinstance(other_values, list) else {other_values}
            )
            self_values_set.discard(None)
            other_values_set.discard(None)
            if self_values_set != other_values_set:
                return False
        return True


class SourceNames(BaseModel):
    """Name strings and schema supplied to the LLM for cleaning and categorisation"""

    entity_schema: str
    original: Names


class DSPySignature(BaseModel):
    instructions: str


class PredictProgramData(BaseModel):
    signature: DSPySignature


def is_empty_string(text: Optional[str | LangText]) -> bool:
    if text is None:
        return True
    if isinstance(text, LangText):
        return is_empty_string(text.text)
    if isinstance(text, str):
        text = text.strip()
        return len(text) == 0
    return False


def name_val_str(name_val: NamesValue) -> str | None:
    if isinstance(name_val, LangText):
        return name_val.text
    return name_val


@cache
def load_single_entity_prompt() -> str:
    with open(SINGLE_ENTITY_PROGRAM_PATH) as program_file:
        program = PredictProgramData.model_validate_json(program_file.read())
        prompt = program.signature.instructions
    return prompt


def clean_names(context: Context, raw_names: SourceNames) -> Names:
    """Use an LLM to clean and categorise names."""
    prompt = load_single_entity_prompt()

    strings = []
    for _prop, names in raw_names.original.nonempty_item_lists():
        for name in names:
            if name not in strings:
                strings.append(name)

    input_data = {"entity_schema": raw_names.entity_schema, "strings": strings}
    input_string = "The entity schema and name strings as JSON:\n\n"
    # ensure_ascii=False so that non-ASCII like Алтайкапиталбанк
    # doesn't get escaped like \u0410\u043b\u0442\u0430\u0439\u043a\u0430\u...
    # which then results in a name in the response like \x041\x041\x041 \x041\x041 \x041
    # probably because gpt4o isn't trained on escape sequences, and we only
    # need it to be JSON-ish embedded in the input string to give it some structure.
    input_string += json.dumps(input_data, indent=2, ensure_ascii=False)

    return run_typed_text_prompt(
        context=context,
        prompt=prompt,
        string=input_string,
        response_type=Names,
        model=LLM_MODEL_VERSION,
    )
