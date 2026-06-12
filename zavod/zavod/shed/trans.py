from typing import Optional, NamedTuple, List
import orjson

from zavod.context import Context
from zavod.entity import Entity
from zavod.exc import ConfigurationException
from zavod.extract.llm import run_text_prompt, DEFAULT_MODEL
from zavod.extract.names.clean import LangText
from zavod import helpers as h


class TransliterationLanguageSpec(NamedTuple):
    """Specification for a transliteration output language."""

    language_code: str
    """The ISO 639-2 language code (e.g., "eng", "rus", "ara")."""

    script: str
    """The writing system/script name (e.g., "Latin", "Cyrillic", "Arabic")."""

    language_name: str
    """The language name for pronunciation (e.g., "English", "Russian", "Arabic")."""


ENGLISH = TransliterationLanguageSpec(
    language_code="eng", script="Latin", language_name="English"
)
RUSSIAN = TransliterationLanguageSpec(
    language_code="rus", script="Cyrillic", language_name="Russian"
)
ARABIC = TransliterationLanguageSpec(
    language_code="ara", script="Arabic", language_name="Arabic"
)


NAME_TRANSLIT_PROMPT = """
Transliterate the following name from the language denoted by the ISO 639-2 Code {code},
 returning a JSON object where

{output_bullets}.

If it looks like a company name, translate the prefix or suffix indicating the legal form,
e.g. the Georgian შპს to Ltd, or the Russian ООО to LLC. If it looks like there are multiple
names, do not separate them in the output but adhere strictly to the output specification above.
"""
POSITION_TRANS_PROMPT = """
Translate the following public office position label from the language denoted by the
 ISO 639-2 code {code}, returning a JSON object where the key 'eng' has the value in
 English.
"""


def make_name_translit_prompt(
    input_code: str, output_specs: List[TransliterationLanguageSpec]
) -> str:
    output_items = []
    for spec in output_specs:
        output_items.append(
            f"- the key '{spec.language_code}' has the value in {spec.script} script for {spec.language_name} pronunciation"
        )
    return NAME_TRANSLIT_PROMPT.format(
        code=input_code, output_bullets="\n".join(output_items)
    )


def make_position_translation_prompt(input_code: str) -> str:
    return POSITION_TRANS_PROMPT.format(code=input_code)


def run_translation_prompt(
    context: Context,
    prompt: str,
    text: str,
    output_langs: List[str] = ["eng"],
    model: str = DEFAULT_MODEL,
) -> List[LangText]:
    """Run a translation/transliteration prompt and return the result as
    LangText instances.

    The prompt must instruct the model to return a JSON object whose keys
    are ISO 639-2 language codes (subset of ``output_langs``) and whose
    values are the translated/transliterated strings. Caching is handled
    by ``run_text_prompt``; the cached entry is invalidated on parse
    failure so a later run can retry.

    Returns an empty list if the LLM is not configured, the response is
    not valid JSON, or the response contains keys outside ``output_langs``.
    Callers are responsible for applying the resulting LangText values to
    an entity.
    """
    try:
        response = run_text_prompt(context, prompt, text, model=model)
    except ConfigurationException as ce:
        context.log.error("LLM translation skipped: %s" % ce.message)
        return []
    try:
        trans_by_lang = orjson.loads(response.content)
    except orjson.JSONDecodeError:
        context.cache.delete(response.cache_key)
        context.log.error(
            "LLM translation returned invalid JSON",
            prompt=prompt,
            text=text,
            model=model,
            response_content=response.content,
        )
        return []
    if not set(trans_by_lang.keys()).issubset(output_langs):
        context.cache.delete(response.cache_key)
        context.log.warning(
            "LLM translation returned unexpected keys",
            prompt=prompt,
            text=text,
            model=model,
            response_content=response.content,
            expected=sorted(output_langs),
        )
        return []
    results: List[LangText] = []
    for lang in output_langs:
        value = trans_by_lang.get(lang)
        if not isinstance(value, str) or not value.strip():
            continue
        results.append(LangText(text=value, lang=lang))
    return results


def apply_translit_names(
    context: Context,
    entity: Entity,
    *,
    input_code: str,
    first_name: str,
    last_name: str,
    output_spec: List[TransliterationLanguageSpec] = [ENGLISH],
    model: str = DEFAULT_MODEL,
) -> None:
    """
    Apply transliterated names to an entity.

    Args:
        context: The context for the operation.
        entity: The entity to which to apply the names.
        input_code: The ISO 639-2 code for the language of the input names.
        first_name: The first name to transliterate.
        last_name: The last name to transliterate.
        output_spec: A list of language specifications for the output names.
    """
    prompt = make_name_translit_prompt(input_code, output_spec)
    output_langs = [spec.language_code for spec in output_spec]
    first_by_lang = {
        lt.lang: lt.text
        for lt in run_translation_prompt(
            context, prompt, first_name, output_langs=output_langs, model=model
        )
    }
    last_by_lang = {
        lt.lang: lt.text
        for lt in run_translation_prompt(
            context, prompt, last_name, output_langs=output_langs, model=model
        )
    }
    for lang in output_langs:
        if lang not in first_by_lang or lang not in last_by_lang:
            continue
        h.apply_name(
            entity,
            first_name=first_by_lang[lang],
            last_name=last_by_lang[lang],
            lang=lang,
            origin=model,
        )


def apply_translit_full_name(
    context: Context,
    entity: Entity,
    input_code: str,
    name: str,
    output: List[TransliterationLanguageSpec] = [ENGLISH],
    prompt: Optional[str] = None,
    alias: bool = False,
    model: str = DEFAULT_MODEL,
) -> None:
    """
    Apply transliterated name to an entity.

    Args:
        context: The context for the operation.
        entity: The entity to which to apply the names.
        input_code: The ISO 639-2 code for the language of the input names.
        name: The name to transliterate.
        output: A list of language specifications for the output names.
        model: GPT model used to translate/transliterate the name.
    """
    if prompt is None:
        prompt = make_name_translit_prompt(input_code, output)
    output_langs = [spec.language_code for spec in output]
    for lt in run_translation_prompt(
        context, prompt, name, output_langs=output_langs, model=model
    ):
        h.apply_name(
            entity,
            full=lt.text,
            lang=lt.lang,
            alias=alias,
            origin=model,
        )
