import logging
from normality import stringify, slugify

log = logging.getLogger(__name__)


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
