import re
import logging
from lxml import etree
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from typing import Any, List, Optional, Tuple
from banal import ensure_list
from datetime import datetime
from normality import stringify, slugify

log = logging.getLogger(__name__)
BRACKETED = re.compile(r"\(.*\)")


def iso_datetime(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def is_empty(text: Optional[str]) -> bool:
    """Check if the given text is empty: it can either be null, or
    the stripped version of the string could have 0 length."""
    if text is None:
        return True
    if isinstance(text, str):
        text = text.strip()
        return len(text) == 0
    return False


def remove_namespace(doc):
    """Remove namespace in the passed XML/HTML document in place and
    return an updated element tree.

    If the namespaces in a document define multiple tags with the same
    local tag name, this will create ambiguity and lead to errors. Most
    XML documents, however, only actively use one namespace."""
    for elem in doc.getiterator():
        elem.tag = etree.QName(elem).localname
    etree.cleanup_namespaces(doc)
    return doc


def jointext(*parts: Tuple[Any], sep=" ") -> str:
    texts: List[str] = []
    for part in parts:
        text = stringify(part)
        if text is not None:
            texts.append(text)
    return sep.join(texts)


def joinslug(*parts, prefix=None, sep="-", strict=True):
    # SLUG_REMOVE = re.compile(r"[<>\\\'\\\"’‘]")
    parts = [slugify(p, sep=sep) for p in parts]
    if strict and None in parts:
        return None
    parts = [p for p in parts if p is not None]
    if not len(parts):
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
    """Sequentially attempt to split a text based on an array of splitting criteria.
    This is useful for strings where multiple separators are used to separate values,
    e.g.: `test,other/misc`. A special case of this is itemised lists like `a) test
    b) other c) misc` which sanction-makers seem to love."""
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


def normalize_url(url: str, params) -> str:
    parsed = urlparse(url)
    query = parse_qsl(parsed.query, keep_blank_values=True)
    if params is not None:
        try:
            params = params.items()
        except AttributeError:
            pass
        query.extend(sorted(params))
    parsed = parsed._replace(query=urlencode(query))
    return urlunparse(parsed)
