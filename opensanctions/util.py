import re
import logging
from banal import ensure_list
from normality import stringify, slugify

log = logging.getLogger(__name__)
BRACKETED = re.compile(r"\(.*\)")


def is_empty(text):
    if text is None:
        return True
    if isinstance(text, str):
        text = text.strip()
        return len(text) < 1
    return False


def jointext(*parts, sep=" "):
    parts = [stringify(p) for p in parts]
    parts = [p for p in parts if p is not None]
    return sep.join(parts)


def joinslug(*parts, prefix=None, sep="-", strict=True):
    # SLUG_REMOVE = re.compile(r"[<>\\\'\\\"’‘]")
    parts = [slugify(p, sep=sep) for p in parts]
    if strict and None in parts:
        return None
    parts = [p for p in parts if p is not None]
    if len(parts) < 1:
        return None
    if prefix is not None:
        prefix = slugify(prefix, sep=sep)
        parts = (prefix, *parts)
    return sep.join(parts)


def remove_bracketed(text):
    """Helps to deal with property values where additional info has been supplied in
    brackets that makes it harder to parse the value. Examples:

    - Russia (former USSR)
    - 1977 (as Muhammad Da'ud Salman)

    It's probably not useful in all of these cases to try and parse and derive meaning
    from the bracketed bit, so we'll just discard it.
    """
    if text is None:
        return
    return BRACKETED.sub(" ", text)


def multi_split(text, splitters):
    fragments = ensure_list(text)
    for splitter in splitters:
        out = []
        for fragment in fragments:
            for frag in fragment.split(splitter):
                frag = frag.strip()
                if len(frag):
                    out.append(frag)
        fragments = out
    return fragments
