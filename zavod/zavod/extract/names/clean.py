from functools import cache
from pathlib import Path
from typing import Any, Generator, List, Optional, Tuple
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
    lang: Optional[str] = None

    def __hash__(self):
        return hash((self.text, self.lang))


NamesValue = str | List[str | LangText | None] | LangText | None


class Names(BaseModel):
    """Names categorised and cleaned of non-name characters."""

    name: NamesValue = None
    alias: NamesValue = None
    weakAlias: NamesValue = None
    previousName: NamesValue = None
    abbreviation: NamesValue = None
    # TODO: Before adding name parts, we should consider whether we should
    # add them directly or construct a full name with them via h.apply_name.
    #
    # Also make sure the LLM and users aren't tempted to infer them.
    #
    # firstName: NamesValue = None
    # middleName: NamesValue = None
    # lastName: NamesValue = None

    def _is_blank_value(self, value: NamesValue) -> bool:
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

    def nonempty_item_lists(self) -> Generator[Tuple[str, List[str]], None, None]:
        """
        Generator yielding each property and a list of any associated non-empty name values.

        Useful when iterating over values in a Names instance.
        """
        for key, value in self.model_dump().items():
            if value is None:
                continue
            if isinstance(value, str):
                if not is_empty_string(value):
                    yield key, [value]
            if isinstance(value, list):
                nonempty_values = [v for v in value if not is_empty_string(v)]
                if nonempty_values:
                    yield key, nonempty_values

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


class DSPyField(BaseModel):
    prefix: str
    description: str


class DSPySignature(BaseModel):
    instructions: str
    fields: List[DSPyField]


class PredictProgramData(BaseModel):
    signature: DSPySignature


def is_empty_string(text: Optional[str]) -> bool:
    if text is None:
        return True
    if isinstance(text, str):
        text = text.strip()
        return len(text) == 0
    return False


@cache
def load_single_entity_prompt() -> str:
    with open(SINGLE_ENTITY_PROGRAM_PATH) as program_file:
        program = PredictProgramData.model_validate_json(program_file.read())
        prompt = program.signature.instructions
        prompt += "\nThe input and output fields are defined as follows:\n"
        for field in program.signature.fields:
            prompt += f"\n{field.prefix}: {field.description}\n"
        # Add the non-DSPy instructions about the expected output format.
        prompt += (
            "\nWhile the output schema can accommodate name values with associated language string, "
            "NEVER infer language of a name string. ONLY indicate language in the language field "
            "if the original input string explicitly indicates the language."
        )

    return prompt


def clean_names(context: Context, raw_names: SourceNames) -> Names:
    """Use an LLM to clean and categorise names."""
    prompt = load_single_entity_prompt()

    strings = []
    for _prop, names in raw_names.original.nonempty_item_lists():
        for name in names:
            if name not in strings:
                strings.append(name)

    input_data = {"strings": strings}
    input_string = "The entity schema and name strings as JSON:\n\n"
    input_string += json.dumps(input_data, indent=2)

    return run_typed_text_prompt(
        context=context,
        prompt=prompt,
        string=input_string,
        response_type=Names,
        model=LLM_MODEL_VERSION,
    )
