from typing import Dict, Tuple

from zavod.context import Context
from zavod.entity import Entity
from zavod.shed.gpt import run_text_prompt
from zavod import helpers as h

NAME_TRANSLIT_PROMPT = """
Transliterate the following name from the language denoted by the ISO 639-2 Code {code},
 returning a JSON object where

{output_bullets}.
"""
POSITION_TRANS_PROMPT = """
Translate the following public office position label from the language denoted by the
 ISO 639-2 code {code}, returning a JSON object where the key 'eng' has the value in
 English.
"""


def make_name_translit_prompt(
    input_code: str, output: Dict[str, Tuple[str, str]]
) -> str:
    output_items = []
    for code, (script, lang) in output.items():
        output_items.append(
            f"- the key '{code}' has the value in {script} script for {lang} pronunciation"
        )
    return NAME_TRANSLIT_PROMPT.format(
        code=input_code, output_bullets="\n".join(output_items)
    )


def make_position_translation_prompt(input_code: str) -> str:
    return POSITION_TRANS_PROMPT.format(code=input_code)


def apply_translit_names(
    context: Context,
    entity: Entity,
    input_code: str,
    first_name: str,
    last_name: str,
    output: Dict[str, Tuple[str, str]] = {"eng": ["Latin", "English"]},
) -> None:
    """
    Apply transliterated names to an entity.

    Args:
        context: The context for the operation.
        entity: The entity to which to apply the names.
        input_code: The ISO 639-2 code for the language of the input names.
        first_name: The first name to transliterate.
        last_name: The last name to transliterate.
        output: A dictionary mapping ISO 639-2 codes to tuples of script and language
    """
    prompt = make_name_translit_prompt(input_code, output)
    first_response = run_text_prompt(context, prompt, first_name)
    last_response = run_text_prompt(context, prompt, last_name)
    for lang in first_response.keys():
        h.apply_name(
            entity,
            first_name=first_response[lang],
            last_name=last_response[lang],
            lang=lang,
        )


def apply_translit_full_name(
    context: Context,
    entity: Entity,
    input_code: str,
    name: str,
    output: Dict[str, Tuple[str, str]] = {"eng": ["Latin", "English"]},
    prompt: str = None,
) -> None:
    """
    Apply transliterated name to an entity.

    Args:
        context: The context for the operation.
        entity: The entity to which to apply the names.
        input_code: The ISO 639-2 code for the language of the input names.
        name: The first name to transliterate.
        output: A dictionary mapping ISO 639-2 codes to tuples of script and language
    """
    if prompt is None:
        prompt = make_name_translit_prompt(input_code, output)
    response = run_text_prompt(context, prompt, name)
    for lang in response.keys():
        h.apply_name(entity, full=response[lang], lang=lang)
