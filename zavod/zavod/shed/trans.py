from functools import cache
from hashlib import sha1
from typing import Optional, NamedTuple, List
import orjson

import google.auth
from google.cloud import translate_v3

from zavod.context import Context
from zavod.entity import Entity
from zavod.exc import ConfigurationException
from zavod.extract.llm import run_text_prompt, DEFAULT_MODEL
from zavod import helpers as h


GOOGLE_TRANSLATE_ORIGIN = "google-translate"


@cache
def _get_google_translation_client() -> translate_v3.TranslationServiceClient:
    return translate_v3.TranslationServiceClient()


@cache
def _get_google_translate_parent() -> str:
    # Picks up project from ADC / GOOGLE_APPLICATION_CREDENTIALS at call time
    # so a missing setup doesn't crash module import.
    _, project_id = google.auth.default()
    return f"projects/{project_id}/locations/global"


def _build_google_translate_cache_key(
    text: str,
    source_language: Optional[str],
    target_language: str,
) -> str:
    cache_hash = sha1(text.encode("utf-8"))
    cache_hash.update(source_language.encode("utf-8") if source_language else b"")
    cache_hash.update(target_language.encode("utf-8"))
    return f"google-translate-{cache_hash.hexdigest()}"


def google_translate(
    context: Context,
    text: str,
    *,
    source_language: Optional[str],
    target_language: str,
    cache_days: int = 100,
) -> str:
    """Translate text via Google Cloud Translate v3, caching the result.

    Language codes are BCP-47 tags. The simplest BCP-47 tag is just a language
    subtag, which for the vast majority of languages is the ISO 639-1 alpha-2
    code — so passing ``"en"``, ``"de"``, ``"ru"`` etc. works fine. Use a
    longer BCP-47 tag when you need to disambiguate script or region (e.g.
    ``"zh-CN"`` vs ``"zh-TW"``, ``"pt-BR"`` vs ``"pt-PT"``, ``"sr-Latn"`` vs
    ``"sr-Cyrl"``). Pass ``source_language=None`` to let Google auto-detect.

    NOTE: this translates rather than transliterates. Names get translated
    literally — e.g. "Al-Qaeda" becomes "the base". Use this only for generic
    phrases like position titles, not for personal or organisation names. For
    those, use a transliteration approach (LLM-based, or Google Translate's
    romanization API).
    """
    cache_key = _build_google_translate_cache_key(
        text, source_language, target_language
    )
    cached_data = context.cache.get_json(cache_key, max_age=cache_days)
    if cached_data is not None:
        return str(cached_data)

    client = _get_google_translation_client()
    response = client.translate_text(
        parent=_get_google_translate_parent(),
        contents=[text],
        mime_type="text/plain",
        source_language_code=source_language if source_language else None,
        target_language_code=target_language,
    )
    if len(response.translations) != 1:
        raise ValueError("Expected exactly one translation from Google Translate")
    translation: str = response.translations[0].translated_text
    context.log.info(
        "Translated text using Google Translate",
        text=text,
        source_language=source_language,
        target_language=target_language,
        translation=translation,
    )
    context.cache.set_json(cache_key, translation)
    return translation


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
    try:
        prompt = make_name_translit_prompt(input_code, output_spec)
        first_name_response = run_text_prompt(context, prompt, first_name, model=model)
        last_name_response = run_text_prompt(context, prompt, last_name, model=model)

        try:
            first_name_trans_by_lang = orjson.loads(first_name_response.content)
            last_name_trans_by_lang = orjson.loads(last_name_response.content)
        except orjson.JSONDecodeError:
            context.cache.delete(first_name_response.cache_key)
            context.cache.delete(last_name_response.cache_key)

            context.log.error(
                "Transliteration failed, returned invalid JSON",
                prompt=prompt,
                first_name=first_name,
                last_name=last_name,
                model=model,
                first_name_response_content=first_name_response.content,
                last_name_response_content=last_name_response.content,
            )
            return

        for spec in output_spec:
            lang = spec.language_code
            if lang not in first_name_trans_by_lang.keys():
                context.log.warning(
                    f"Transliteration for name did not return a value for {lang}. Will skip applying the transliterated name.",
                    prompt=prompt,
                    first_name=first_name,
                    model=model,
                    response=repr(first_name_response),
                )
                context.cache.delete(first_name_response.cache_key)
                continue
            if lang not in last_name_trans_by_lang.keys():
                context.log.warning(
                    f"Transliteration for {last_name} did not return a value for {lang}. Will skip applying the transliterated name.",
                    prompt=prompt,
                    last_name=last_name,
                    model=model,
                    response=repr(last_name_response),
                )
                context.cache.delete(last_name_response.cache_key)
                continue

            h.apply_name(
                entity,
                first_name=first_name_trans_by_lang[lang],
                last_name=last_name_trans_by_lang[lang],
                lang=lang,
                origin=model,
            )
    except ConfigurationException as ce:
        context.log.error("Transliteration failed: %s" % ce.message)


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
    try:
        if prompt is None:
            prompt = make_name_translit_prompt(input_code, output)
        response = run_text_prompt(context, prompt, name, model=model)
        try:
            trans_by_lang = orjson.loads(response.content)
        except orjson.JSONDecodeError:
            context.cache.delete(response.cache_key)
            context.log.error(
                "Transliteration failed, returned invalid JSON",
                prompt=prompt,
                name=name,
                model=model,
                response_content=response.content,
            )
            return

        output_codes = {spec.language_code for spec in output}
        if not set(trans_by_lang.keys()).issubset(output_codes):
            context.log.warning(
                f"Transliteration for {name} returned unexpected keys. Will skip applying the transliterated name.",
                prompt=prompt,
                name=name,
                model=model,
                response=repr(response),
                output=repr(output),
            )
            context.cache.delete(response.cache_key)
            return

        for lang, transliteration in trans_by_lang.items():
            h.apply_name(
                entity,
                full=transliteration,
                lang=lang,
                alias=alias,
                origin=model,
            )
    except ConfigurationException as ce:
        context.log.error("Transliteration failed: %s" % ce.message)
