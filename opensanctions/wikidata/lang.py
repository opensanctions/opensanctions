from typing import Counter, Dict, Optional


DEFAULT_LANG = "en"
ALT_LANG_ORDER = ["es", "fr", "de", "ru"]


def pick_lang_text(values: Dict[str, str]) -> Optional[str]:
    """Pick a text value from a dict of language -> text."""
    value = values.get(DEFAULT_LANG)
    if value is not None:
        return value

    counter = Counter[str]()
    counter.update(values.values())
    for (value, count) in counter.most_common(1):
        if count > 1:
            return value

    for lang in ALT_LANG_ORDER:
        value = values.get(lang)
        if value is not None:
            return value

    for value in values.values():
        if value is not None:
            return value

    return None


def pick_obj_lang(items: Dict[str, Dict[str, str]]) -> Optional[str]:
    values = {}
    for label in items.values():
        value = label.get("value")
        if value is not None:
            values[label.get("language", "")] = value
    return pick_lang_text(values)
