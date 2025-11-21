from functools import cache
from pathlib import Path
from typing import List

from pydantic import BaseModel
from zavod.context import Context
from zavod.extract.llm import run_typed_text_prompt

LLM_MODEL_VERSION = "gpt-4o"
SINGLE_ENTITY_PROGRAM_PATH = Path(__file__).parent / "dspy/single_entity_program.json"


class CleanNames(BaseModel):
    """Names categorised and cleaned of non-name characters."""

    full_name: List[str]
    alias: List[str]
    weak_alias: List[str]
    previous_name: List[str]


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


def clean_names(context: Context, string: str) -> CleanNames:
    prompt = load_single_entity_prompt()
    return run_typed_text_prompt(
        context=context,
        prompt=prompt,
        string=f"Input string: {string}",
        response_type=CleanNames,
        model=LLM_MODEL_VERSION,
    )
