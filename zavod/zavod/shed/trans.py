from typing import NamedTuple, Optional, List

from pydantic import BaseModel

from zavod.context import Context
from zavod.entity import Entity
from zavod.exc import ConfigurationException
from zavod.extract.llm import run_typed_text_prompt, DEFAULT_MODEL
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
Transliterate the following name from the language denoted by the ISO 639-2 Code {code}.
Return one Translation entry per requested output language:

{output_bullets}.

If it looks like a company name, translate the prefix or suffix indicating the legal form,
e.g. the Georgian შპს to Ltd, or the Russian ООО to LLC. If it looks like there are multiple
names, do not separate them in the output but adhere strictly to the output specification above.
"""
POSITION_TRANS_PROMPT = """
Translate the following public office position label from the language denoted by the
 ISO 639-2 code {code} into English. Return a single Translation entry with lang='eng'.
"""


def make_name_translit_prompt(
    input_code: str, output_specs: List[TransliterationLanguageSpec]
) -> str:
    output_items = []
    for spec in output_specs:
        output_items.append(
            f"- lang='{spec.language_code}', text in {spec.script} script for {spec.language_name} pronunciation"
        )
    return NAME_TRANSLIT_PROMPT.format(
        code=input_code, output_bullets="\n".join(output_items)
    )


def make_position_translation_prompt(input_code: str) -> str:
    return POSITION_TRANS_PROMPT.format(code=input_code)


class Translation(BaseModel):
    lang: str
    """ISO 639-2 (3-letter) language code."""
    text: str
    """The translated/transliterated value for ``lang``."""


class TranslationResponse(BaseModel):
    """JSON response schema for translation/transliteration prompts.

    Translations are returned as a list of ``Translation`` entries rather
    than as named fields per language. This keeps the schema generic
    across any ISO 639-2 code without enumerating them — per-call the
    prompt tells the LLM which languages to produce, and
    ``run_translation_prompt`` filters the returned entries against the
    caller's ``output_langs``.

    Caching caveat: ``run_typed_text_prompt`` includes this model's JSON
    schema in the cache key, so changing the shape of ``Translation`` or
    ``TranslationResponse`` (renaming/adding/removing fields) invalidates
    *every* cached translation response across every crawler. Adding a
    new output language does not trigger this — only structural changes
    do.
    """

    translations: List[Translation]


def run_translation_prompt(
    context: Context,
    prompt: str,
    text: str,
    output_langs: List[str] = ["eng"],
    model: str = DEFAULT_MODEL,
) -> List[LangText]:
    """Run a translation/transliteration prompt and return the result as
    LangText instances.

    The prompt should describe what to translate and which output
    languages are expected; the response shape is enforced via OpenAI
    structured outputs against ``TranslationResponse`` so the prompt
    does not need to spell out the JSON format. Caching is handled by
    ``run_typed_text_prompt``.

    Returns ``[]`` if the LLM is not configured. Entries with unrequested
    languages, missing/empty text, or duplicate language codes (first
    wins) are dropped, so the result may have fewer entries than
    ``output_langs``. Callers are responsible for applying the resulting
    LangText values to an entity.
    """
    try:
        response = run_typed_text_prompt(
            context, prompt, text, response_type=TranslationResponse, model=model
        )
    except ConfigurationException as ce:
        context.log.error("LLM translation skipped: %s" % ce.message)
        return []
    requested = set(output_langs)
    seen: set[str] = set()
    results: List[LangText] = []
    for entry in response.translations:
        if entry.lang not in requested or entry.lang in seen:
            continue
        if not entry.text.strip():
            continue
        seen.add(entry.lang)
        results.append(LangText(text=entry.text, lang=entry.lang))
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
    first_texts = run_translation_prompt(
        context, prompt, first_name, output_langs=output_langs, model=model
    )
    last_texts = run_translation_prompt(
        context, prompt, last_name, output_langs=output_langs, model=model
    )
    first_by_lang = {lt.lang: lt.text for lt in first_texts}
    last_by_lang = {lt.lang: lt.text for lt in last_texts}
    for lang in output_langs:
        if lang not in first_by_lang:
            context.log.warning(
                f"Transliteration for first name did not return a value for "
                f"{lang}. Will skip applying the transliterated name.",
                prompt=prompt,
                first_name=first_name,
                model=model,
            )
            continue
        if lang not in last_by_lang:
            context.log.warning(
                f"Transliteration for {last_name} did not return a value for "
                f"{lang}. Will skip applying the transliterated name.",
                prompt=prompt,
                last_name=last_name,
                model=model,
            )
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
    texts = run_translation_prompt(
        context, prompt, name, output_langs=output_langs, model=model
    )
    for lt in texts:
        h.apply_name(
            entity,
            full=lt.text,
            lang=lt.lang,
            alias=alias,
            origin=model,
        )
