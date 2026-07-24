from dataclasses import dataclass
from typing import NamedTuple
from collections.abc import Sequence
import orjson

from zavod.context import Context
from zavod.entity import Entity
from zavod.exc import ConfigurationException
from zavod.extract.llm import run_text_prompt, DEFAULT_MODEL
from zavod.util import LangText
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

PREFERRED_LANGUAGE = ENGLISH
"""Default transliteration output language for OpenSanctions data."""


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
 ISO 639-2 code {code} into English, returning a JSON object where the key 'eng' has
 the value in English.

Keep place names (countries, cities, regions, administrative areas) intact rather than
 translating their meaning. Only replace a place name with its established English
 exonym for very common, widely recognised cases — e.g. "Россия" → "Russia", "Москва"
 → "Moscow", "Wien" → "Vienna", "München" → "Munich". For anything less obvious, err
 on the side of keeping the original place name: if it is already in Latin script,
 keep it verbatim (e.g. "São Paulo" stays "São Paulo", not "Saint Paul"); if it is in
 a non-Latin script, transliterate it to Latin script rather than translating it
 (e.g. "Карачаево-Черкесия" → "Karachayevo-Cherkesiya", not "Karachay-Cherkessia").
"""


def make_name_translit_prompt(
    input_code: str, output_specs: Sequence[TransliterationLanguageSpec]
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


@dataclass(frozen=True, kw_only=True)
class TranslationResult:
    texts: list[LangText]
    cache_key: str | None
    """Cache key of the underlying run_text_prompt response, set only when
    the response was parsed and accepted. Callers that do additional
    per-result validation can drop this entry via context.cache.delete()
    so a later run can retry."""
    origin: str
    """The model that produced the translation. Suitable for passing as the
    ``origin`` when applying the resulting values to an entity."""

    def get_preferred_language(self) -> LangText | None:
        """Return the ``LangText`` for the preferred output language, or None
        if absent. The preferred language is currently English."""
        for text in self.texts:
            if text.lang == "eng":
                return text
        return None


def run_translation_prompt(
    context: Context,
    *,
    prompt: str,
    text: str,
    output_langs: list[str] = ["eng"],
    model: str = DEFAULT_MODEL,
) -> TranslationResult:
    """Run a translation/transliteration prompt and return the result as
    LangText instances together with the underlying response's cache key.

    The prompt must instruct the model to return a JSON object whose keys
    are ISO 639-2 language codes (subset of ``output_langs``) and whose
    values are the translated/transliterated strings. Caching is handled
    by ``run_text_prompt``; the cached entry is invalidated on parse
    failure so a later run can retry.

    Returns ``TranslationResult([], None)`` if the LLM is not configured,
    the response is not valid JSON, or the response contains keys outside
    ``output_langs``. On the success path ``cache_key`` is set so callers
    can invalidate the response themselves if their own per-result
    validation fails. Callers are responsible for applying the resulting
    LangText values to an entity.
    """
    try:
        response = run_text_prompt(context, prompt, text, model=model)
    except ConfigurationException as ce:
        context.log.error(f"LLM translation skipped: {ce.message}")
        return TranslationResult(texts=[], cache_key=None, origin=model)
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
        return TranslationResult(texts=[], cache_key=None, origin=model)
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
        return TranslationResult(texts=[], cache_key=None, origin=model)
    results: list[LangText] = []
    for lang in output_langs:
        value = trans_by_lang.get(lang)
        if not isinstance(value, str) or not value.strip():
            continue
        results.append(LangText(text=value, lang=lang))
    return TranslationResult(texts=results, cache_key=response.cache_key, origin=model)


def translate_position_name(
    context: Context,
    label: LangText,
    *,
    model: str = DEFAULT_MODEL,
) -> TranslationResult:
    """Translate a public office position label into English.

    ``label`` carries the source text and its language in ``label.lang``,
    which must be set. Builds the position-translation prompt for that
    language and runs it. Use ``result.get_preferred_language()`` to read the
    ``LangText`` (None if none was produced) and ``result.origin`` as the
    ``origin`` when applying it to an entity.
    """
    assert label.lang is not None, "Source language is required for translation"
    prompt = make_position_translation_prompt(label.lang)
    return run_translation_prompt(context, prompt=prompt, text=label.text, model=model)


def apply_translit_names(
    context: Context,
    entity: Entity,
    *,
    input_code: str,
    first_name: str,
    last_name: str,
    output_spec: list[TransliterationLanguageSpec] = [ENGLISH],
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
    first_result = run_translation_prompt(
        context,
        prompt=prompt,
        text=first_name,
        output_langs=output_langs,
        model=model,
    )
    last_result = run_translation_prompt(
        context,
        prompt=prompt,
        text=last_name,
        output_langs=output_langs,
        model=model,
    )
    first_by_lang = {lt.lang: lt.text for lt in first_result.texts}
    last_by_lang = {lt.lang: lt.text for lt in last_result.texts}
    for lang in output_langs:
        if lang not in first_by_lang:
            context.log.warning(
                f"Transliteration for first name did not return a value for "
                f"{lang}. Will skip applying the transliterated name.",
                prompt=prompt,
                first_name=first_name,
                model=model,
            )
            if first_result.cache_key is not None:
                context.cache.delete(first_result.cache_key)
            continue
        if lang not in last_by_lang:
            context.log.warning(
                f"Transliteration for {last_name} did not return a value for "
                f"{lang}. Will skip applying the transliterated name.",
                prompt=prompt,
                last_name=last_name,
                model=model,
            )
            if last_result.cache_key is not None:
                context.cache.delete(last_result.cache_key)
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
    name: LangText,
    *,
    output: Sequence[TransliterationLanguageSpec] = (PREFERRED_LANGUAGE,),
    prompt: str | None = None,
    alias: bool = False,
    model: str = DEFAULT_MODEL,
) -> None:
    """Apply a transliterated name to an entity.

    ``name`` carries the source text and its language in ``name.lang``, which
    must be set. Each spec in ``output`` produces one transliterated name
    applied to the entity; the default transliterates into the preferred
    output language only.

    Args:
        context: The context for the operation.
        entity: The entity to which to apply the names.
        name: The name to transliterate, with its source language.
        output: The language specifications for the output names.
        prompt: An optional override for the transliteration prompt.
        alias: Whether to apply the results as aliases rather than names.
        model: GPT model used to translate/transliterate the name.
    """
    assert name.lang is not None, "Source language is required for transliteration"
    if prompt is None:
        prompt = make_name_translit_prompt(name.lang, output)
    output_langs = [spec.language_code for spec in output]
    result = run_translation_prompt(
        context, prompt=prompt, text=name.text, output_langs=output_langs, model=model
    )
    for lang_text in result.texts:
        h.apply_name(
            entity,
            full=lang_text.text,
            lang=lang_text.lang,
            alias=alias,
            origin=model,
        )
