from functools import cache
from pathlib import Path
from typing import Generator, List, Optional, Tuple

from pydantic import BaseModel
from zavod.context import Context
from zavod.extract.llm import run_typed_text_prompt

LLM_MODEL_VERSION = "gpt-4o"
SINGLE_ENTITY_PROGRAM_PATH = Path(__file__).parent / "dspy/single_entity_program.json"
# TODO: try and improve on this
# The idea was that these are fields, because the crawler can decide whether full_name
# goes to the `name` or the `alias` property based on whether the value comes from a
# full name or an alias field. But maybe that's as much a suggestion as the irregularity
# check is.
PROP_TO_FIELD = {
    "name": "full_name",
    "alias": "alias",
    "weakAlias": "weak_alias",
    "previousName": "previous_name",
    "abbreviation": "abbreviation",
}

NamesValue = str | List[str] | None


class RawNames(BaseModel):
    """Name strings and schema supplied to the LLM for cleaning and categorisation"""

    entity_schema: str
    strings: List[str]


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
        return all(
            (
                value is None
                or (isinstance(value, str) and is_empty_string(value))
                or (isinstance(value, list) and all(is_empty_string(v) for v in value))
            )
            for value in self.model_dump().values()
        )

    def item_lists(self) -> Generator[Tuple[str, List[str]], None, None]:
        for key, value in self.model_dump().items():
            if value is None:
                continue
            if isinstance(value, str):
                yield key, [value]
            if isinstance(value, list):
                yield key, value

    def simplify(self) -> "Names":
        """Simplify the names by converting single-item lists to strings."""
        for key, value in self.model_dump().items():
            if isinstance(value, list) and len(value) == 1:
                self.__setattr__(key, value[0])


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


def clean_names(context: Context, raw_names: RawNames) -> Names:
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
