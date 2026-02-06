from functools import cache
from pathlib import Path
from typing import Generator, List, Optional, Tuple

from pydantic import BaseModel, JsonValue
from zavod.context import Context
from zavod.extract.llm import run_typed_text_prompt

LLM_MODEL_VERSION = "gpt-4o"
SINGLE_ENTITY_PROGRAM_PATH = Path(__file__).parent / "dspy/single_entity_program.json"


NamesValue = str | List[str] | None


def is_empty_string(text: Optional[str]) -> bool:
    if text is None:
        return True
    if isinstance(text, str):
        text = text.strip()
        return len(text) == 0
    return False


class Names(BaseModel):
    """Names categorised and cleaned of non-name characters."""

    name: NamesValue = None
    alias: NamesValue = None
    weakAlias: NamesValue = None
    previousName: NamesValue = None
    abbreviation: NamesValue = None
    firstName: NamesValue = None
    middleName: NamesValue = None
    lastName: NamesValue = None

    def is_empty(self) -> bool:
        for prop, names in self.nonempty_item_lists():
            return False
        return True

    def nonempty_item_lists(self) -> Generator[Tuple[str, List[str]], None, None]:
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
        """Simplify the names by converting single-item lists to strings.
        This is useful for formatting for human editing in reviews."""
        for key, value in self.model_dump().items():
            if isinstance(value, list) and len(value) == 1:
                self.__setattr__(key, value[0])


class SourceNames(BaseModel):
    """Name strings and schema supplied to the LLM for cleaning and categorisation"""

    entity_schema: str
    original: Names

    def nonempty_values_dict(self) -> JsonValue:
        """Return a dictionary of non-empty name values."""
        result = {"entity_schema": self.entity_schema}
        for prop, names in self.original.nonempty_item_lists():
            nonempty_names = [name for name in names if not is_empty_string(name)]
            if nonempty_names:
                result[prop] = nonempty_names
        return result


class DSPySignature(BaseModel):
    instructions: str


class PredictProgramData(BaseModel):
    signature: DSPySignature


@cache
def load_single_entity_prompt() -> str:
    with open(SINGLE_ENTITY_PROGRAM_PATH) as program_file:
        program = PredictProgramData.model_validate_json(program_file.read())
        prompt = program.signature.instructions
    return prompt


def clean_names(context: Context, raw_names: SourceNames) -> Names:
    """Use an LLM to clean and categorise names."""
    prompt = load_single_entity_prompt()
    return run_typed_text_prompt(
        context=context,
        prompt=prompt,
        string="The entity schema and name strings as JSON:\n\n"
        + raw_names.model_dump_json(),
        response_type=Names,
        model=LLM_MODEL_VERSION,
    )
