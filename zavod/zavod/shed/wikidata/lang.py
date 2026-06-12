from typing import Optional

import orjson
from nomenklatura.wikidata.lang import LangText

from zavod.context import Context
from zavod.exc import ConfigurationException
from zavod.extract.llm import DEFAULT_MODEL, run_text_prompt
from zavod.shed.trans import make_position_translation_prompt


def translate_position_label_to_english(
    context: Context,
    langtext: LangText,
    model: str = DEFAULT_MODEL,
) -> Optional[LangText]:
    """Translate a Wikidata position-label ``LangText`` into English via the LLM.

    Uses the position-specific translation prompt from
    ``zavod.shed.trans.make_position_translation_prompt``, which translates
    role/office words literally but keeps place names intact (transliterating
    non-Latin scripts to Latin instead of translating them).

    Returns a new ``LangText`` with ``lang="eng"`` and the translated text, or
    ``None`` if the input has no usable text/lang, the LLM is not configured,
    or the response cannot be parsed. The cached response is invalidated in
    the failure path so a later run can retry.
    """
    if langtext.text is None or langtext.lang is None:
        return None
    prompt = make_position_translation_prompt(langtext.lang)
    try:
        response = run_text_prompt(context, prompt, langtext.text, model=model)
    except ConfigurationException as ce:
        context.log.error("Position label translation skipped: %s" % ce.message)
        return None
    try:
        data = orjson.loads(response.content)
    except orjson.JSONDecodeError:
        context.cache.delete(response.cache_key)
        context.log.error(
            "Position label translation returned invalid JSON",
            label=langtext.text,
            lang=langtext.lang,
            response_content=response.content,
        )
        return None
    translated = data.get("eng")
    if not isinstance(translated, str) or not translated.strip():
        context.cache.delete(response.cache_key)
        context.log.warning(
            "Position label translation missing 'eng' value",
            label=langtext.text,
            lang=langtext.lang,
            response_content=response.content,
        )
        return None
    return LangText(translated, lang="eng", original=langtext.original)
