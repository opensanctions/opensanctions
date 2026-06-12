from typing import Optional

from nomenklatura.wikidata.lang import LangText
from rigour.langs import iso_639_alpha2

from zavod.context import Context
from zavod.shed.trans import google_translate


def translate_langtext_to_english(
    context: Context, langtext: LangText
) -> Optional[LangText]:
    """Translate a ``LangText`` to English via Google Translate.

    Returns a new ``LangText`` (``lang="eng"``) carrying the translated text,
    with ``original`` preserved from the input. Returns ``None`` if the input
    has no text, no source language, or no ISO 639 alpha-2 mapping we can
    feed to Google Translate.
    """
    if langtext.text is None or langtext.lang is None:
        return None
    source_alpha2 = iso_639_alpha2(langtext.lang)
    if source_alpha2 is None:
        return None
    translated = google_translate(
        context,
        langtext.text,
        source_language=source_alpha2,
        target_language="en",
    )
    return LangText(translated, lang="eng", original=langtext.original)
