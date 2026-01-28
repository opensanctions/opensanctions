from functools import cache
from hashlib import sha1
from typing import Optional, List

from google.cloud import translate_v3
import google.auth

import rigour.langs

from zavod.context import Context
from zavod.entity import Entity
from zavod import helpers as h


# BCP-47 language codes for caller convenience
ENGLISH = "en-US"
RUSSIAN = "ru-RU"
CHINESE = "zh-CN"

_, PROJECT_ID = google.auth.default()
# Magic string to make the Google Translate API happy.
GOOGLE_TRANSLATE_PARENT = f"projects/{PROJECT_ID}/locations/global"

# https://docs.cloud.google.com/translate/docs/languages#roman
# For these languages, we automatically enable "Romanization" support.
# They don't really say how that's better, but they say it's better. Trust us bro.
GOOGLE_TRANSLATE_ROMANIZATION_SUPPORTED_LANGUAGES = [
    "ar",  # Arabic
    "am",  # Amharic
    "bn",  # Bengali
    "be"  # Belarusian
    "gu",  # Gujarati
    "hi",  # Hindi
    "ja",  # Japanese
    "kn",  # Kannada
    "my",  # Myanmar
    "ru",  # Russian
    "sr",  # Serbian
    "ta",  # Tamil
    "te",  # Telugu
    "uk",  # Ukrainian
]


# Magic string to put in the "origin" field of the statements.
GOOGLE_TRANSLATE_ORIGIN = "google-translate"


@cache
def get_google_translation_client() -> translate_v3.TranslationServiceClient:
    return translate_v3.TranslationServiceClient()


def build_cache_key(
    prefix: str,
    text: str,
    source_language: Optional[str],
    target_language: str,
) -> str:
    cache_hash = sha1(text.encode("utf-8"))
    cache_hash.update(source_language.encode("utf-8") if source_language else b"")
    cache_hash.update(target_language.encode("utf-8"))
    return f"{prefix}-{cache_hash.hexdigest()}"


def google_translate(
    context: Context,
    text: str,
    *,
    source_language: Optional[str],
    target_language: str,
    # TOOD(Leon Handreke): This is what we use by default for LLM, but those evolve quickly
    # (also we like giving Sam Altman some dollars to dry his tears).
    # Should this be longer?
    cache_days: int = 100,
) -> str:
    cache_key = build_cache_key(
        "google-translate", text, source_language, target_language
    )
    cached_data = context.cache.get_json(cache_key, max_age=cache_days)
    if cached_data is not None:
        context.log.info(
            "Google Translate cache hit",
            text=text,
            source_language=source_language,
            target_language=target_language,
            translation=cached_data,
        )
        return cached_data

    client = get_google_translation_client()
    response = client.translate_text(
        parent=GOOGLE_TRANSLATE_PARENT,
        contents=[text],
        mime_type="text/plain",
        source_language_code=source_language if source_language else None,
        target_language_code=target_language,
    )
    if len(response.translations) != 1:
        raise ValueError("Expected exactly one translation from Google Translate")
    translation = response.translations[0].translated_text
    context.log.info(
        "Translated text using Google Translate",
        text=text,
        source_language=source_language,
        target_language=target_language,
        translation=translation,
    )
    context.cache.set_json(cache_key, translation)
    return translation


# def google_translate_romanize(
#     context: Context,
#     *,
#     text: str,
#     source_language: str,
#     # TOOD(Leon Handreke): This is what we use by default for LLM, but those evolve quickly
#     # (also we like giving Sam Altman some dollars to dry his tears).
#     # Should this be longer?
#     cache_days: int = 100,
# ) -> str:
#     """
#     Romanize text using Google Translate.

#     Romanization is a feature of the Google Translate API that converts text from a non-Latin script to a Latin script,
#     which is why we don't need to specify an explicit target language. Romanization is only supported for a limited set of languages.
#     """
#     assert source_language.split("-")[0] in GOOGLE_TRANSLATE_ROMANIZATION_SUPPORTED_LANGUAGES, "Source language must be in the list of languages that support Romanization"

#     cache_key = build_cache_key("google-translate-romanize", text, source_language, target_language="")
#     cached_data = context.cache.get_json(cache_key, max_age=cache_days)
#     if cached_data is not None:
#         context.log.info("Google Translate romanization cache hit", text=text, source_language=source_language, romanization=cached_data)
#         return cached_data

#     client = get_google_translation_client()
#     response = client.romanize_text(
#         request=RomanizeTextRequest(
#             parent=GOOGLE_TRANSLATE_PARENT,
#             contents=[text],
#             # Of course, the Romanization API wants a special cookie and doesn't expect a BCP-47 language code,
#             # but just the first party (which is ISO 639 Alpha-2 mostly). Sigh.
#             source_language_code=source_language.split("-")[0],
#         )

#     )
#     if len(response.romanizations) != 1:
#         raise ValueError("Expected exactly one romanization from Google Translate")
#     romanization = response.romanizations[0].romanized_text
#     context.log.info("Romanized text using Google Translate", text=text, source_language=source_language, romanization=romanization)
#     context.cache.set_json(cache_key, romanization)
#     return romanization


# def google_translate_romanize_if_supported(
#     context: Context,
#     *,
#     text: str,
#     source_language: str,
#     target_language: str,
# ) -> str:
#     # If the input language is in the list of languages that support Romanization, and the output language is English
#     # trigger the magic "Romanization" feature.
#     # TODO(Leon Handreke): This is a hack. Does it make sense to apply the "eng" language tag on emit in this case?
#     is_romanization_supported = source_language.split("-")[0] in GOOGLE_TRANSLATE_ROMANIZATION_SUPPORTED_LANGUAGES
#     # if is_romanization_supported and target_language == ENGLISH:
#     #     return google_translate_romanize(context, text=text, source_language=source_language)
#     return google_translate(context, text=text, source_language=source_language, target_language=target_language)


def get_iso639_alpha3_from_bcp47_language_code(
    context: Context, bcp47_language_code: str
) -> Optional[str]:
    """
    Convert a BCP-47 language code to an ISO 639 Alpha-3 code for use in FtM statements.
    """
    lang_iso639_alpha3 = rigour.langs.iso_639_alpha3(bcp47_language_code.split("-")[0])
    if lang_iso639_alpha3 is None:
        context.log.warning(
            "Failed to convert BCP-47 language code to ISO 639 Alpha-3 for statement language code",
            bcp47_language_code=bcp47_language_code,
        )
        # That's fine, the statement will just not have a language code.

    return lang_iso639_alpha3


def apply_translit_names(
    context: Context,
    entity: Entity,
    *,
    source_language: str,
    first_name: str,
    last_name: str,
    target_languages: List[str] = [ENGLISH],
) -> None:
    """
    Apply transliterated names to an entity.

    Args:
        context: The context for the operation.
        entity: The entity to which to apply the names.
        input_code: The BCP-47 language code for the language of the input names.
        first_name: The first name to transliterate.
        last_name: The last name to transliterate.
        output_languages: A list of BCP-47 language codes for the output names.
    """
    for target_language in target_languages:
        translated_first_name = google_translate(
            context,
            text=first_name,
            source_language=source_language,
            target_language=target_language,
        )
        translated_last_name = google_translate(
            context,
            text=last_name,
            source_language=source_language,
            target_language=target_language,
        )

        h.apply_name(
            entity,
            first_name=translated_first_name,
            last_name=translated_last_name,
            lang=get_iso639_alpha3_from_bcp47_language_code(context, target_language),
            origin=GOOGLE_TRANSLATE_ORIGIN,
        )


def apply_translit_full_name(
    context: Context,
    entity: Entity,
    *,
    input_language: str,
    name: str,
    output_languages: List[str] = [ENGLISH],
    alias: bool = False,
) -> None:
    """
    Apply transliterated name to an entity.

    Args:
        context: The context for the operation.
        entity: The entity to which to apply the names.
        input_language: The BCP-47 language code for the language of the input names.
        name: The name to transliterate.
        output_languages: A list of BCP-47 language codes for the output names.
        alias: Whether the name should be applied as an alias.
    """
    for output_language in output_languages:
        translated_name = google_translate(
            context,
            text=name,
            source_language=input_language,
            target_language=output_language,
        )
        h.apply_name(
            entity,
            full=translated_name,
            lang=get_iso639_alpha3_from_bcp47_language_code(context, output_language),
            alias=alias,
            origin=GOOGLE_TRANSLATE_ORIGIN,
        )
