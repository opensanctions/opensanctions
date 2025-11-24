from functools import cache
from pathlib import Path
from typing import List, Optional

from pydantic import BaseModel
from zavod import settings
from zavod.context import Context
from zavod.entity import Entity
from zavod.extract.llm import run_typed_text_prompt
from zavod.helpers.names import is_name_irregular
from zavod.stateful.review import JSONSourceValue, Review, review_extraction

LLM_MODEL_VERSION = "gpt-4o"
SINGLE_ENTITY_PROGRAM_PATH = Path(__file__).parent / "dspy/single_entity_program.json"


class RawNames(BaseModel):
    """Name strings and schema supplied to the LLM for cleaning and categorisation"""

    entity_schema: str
    strings: List[str]


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


def clean_names(context: Context, raw_names: RawNames) -> CleanNames:
    prompt = load_single_entity_prompt()
    return run_typed_text_prompt(
        context=context,
        prompt=prompt,
        string="The entity schema and name strings as JSON:\n\n"
        + raw_names.model_dump_json(),
        response_type=CleanNames,
        model=LLM_MODEL_VERSION,
    )


def apply_names(
    entity: Entity,
    strings: List[Optional[str]],
    review: Review[CleanNames],
    alias: bool = False,
    lang: Optional[str] = None,
) -> None:
    field_props = [
        ("full_name", "alias" if alias else "name"),
        ("alias", "alias"),
        ("weak_alias", "weakAlias"),
        ("previous_name", "previousName"),
    ]
    if not review.accepted:
        prop = "alias" if alias else "name"
        for string in strings:
            entity.add(prop, string, lang=lang)
        return

    for field_name, prop in field_props:
        for name in getattr(review.extracted_data, field_name):
            entity.add(
                prop,
                name,
                lang=lang,
                origin=review.origin,
                original_value=review.source_value,
            )


def review_names(
    context: Context,
    entity: Entity,
    strings: List[Optional[str]],
    lang: Optional[str] = None,
    alias: bool = False,
) -> Optional[Review[CleanNames]]:
    """
    Clean names if needed, then post them for review.

    Args:
        context: The current context.
        entity: The entity to apply names to.
        string: The raw name(s) string.
        alias: If this is known to be an alias and not a primary name.
        lang: The language of the name, if known.
    """
    strings = [s for s in strings if s]

    if not strings:
        return None

    if settings.CI or not any(is_name_irregular(entity, s) for s in strings):
        prop = "alias" if alias else "name"
        for string in strings:
            entity.add(prop, string, lang=lang)
        return None

    non_blank_strings = [s for s in strings if s and s.strip()]
    raw_names = RawNames(entity_schema=entity.schema.name, strings=non_blank_strings)
    names = clean_names(context, raw_names)

    source_value = JSONSourceValue(
        key_parts=[entity.schema.name] + non_blank_strings,
        label="names",
        data=raw_names.model_dump(),
    )
    review = review_extraction(
        context,
        source_value=source_value,
        original_extraction=names,
        origin=LLM_MODEL_VERSION,
    )
    return review


def apply_reviewed_names(
    context: Context,
    entity: Entity,
    strings: List[Optional[str]],
    lang: Optional[str] = None,
    alias: bool = False,
) -> None:
    """
    Clean names if needed, then post them for review.
    Cleaned names are applied to an entity if accepted, falling back
    to applying the original string as the name or alias if not.

    Also falls back to applying the original string if the CI environment
    variable is truthy, so that crawlers using this can run in CI.

    Args:
        context: The current context.
        entity: The entity to apply names to.
        string: The raw name(s) string.
        alias: If this is known to be an alias and not a primary name.
        lang: The language of the name, if known.
    """
    review = review_names(context, entity, strings, lang=lang, alias=alias)
    if review is None:
        return
    apply_names(entity, strings, review, alias=alias, lang=lang)
