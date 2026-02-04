from functools import cache
from pathlib import Path
from typing import List

from pydantic import BaseModel
from zavod.context import Context
from zavod.extract.llm import run_typed_text_prompt

LLM_MODEL_VERSION = "gpt-4o"
SINGLE_ENTITY_PROGRAM_PATH = Path(__file__).parent / "dspy/single_entity_program.json"
# The idea was that these are fields, because the crawler can decide whether full_name
# goes to the `name` or the `alias` property based on whether the value comes from a
# full name or an alias field. But maybe that's as much a suggestion as the irregularity
# check is.
PROP_TO_FIELD = {
    "name": "full_name",
    "alias": "alias",
    "weakAlias": "weak_alias",
    "previousName": "previous_name",
}


class RawNames(BaseModel):
    """Name strings and schema supplied to the LLM for cleaning and categorisation"""

    entity_schema: str
    strings: List[str]


class CleanNames(BaseModel):
    """Names categorised and cleaned of non-name characters."""

    full_name: List[str] = []
    alias: List[str] = []
    weak_alias: List[str] = []
    previous_name: List[str] = []


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


def clean_names(context: Context, raw_names: RawNames) -> CleanNames:
    """Use an LLM to clean and categorise names."""
    prompt = load_single_entity_prompt()
    return run_typed_text_prompt(
        context=context,
        prompt=prompt,
        string="The entity schema and name strings as JSON:\n\n"
        + raw_names.model_dump_json(),
        response_type=CleanNames,
        model=LLM_MODEL_VERSION,
    )
