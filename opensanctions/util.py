import re
import logging
from lxml import etree
from banal import ensure_list
from datetime import datetime, date
from normality import stringify, slugify

log = logging.getLogger(__name__)
BRACKETED = re.compile(r"\(.*\)")
YEAR = 4
MONTH = 7
DAY = 10
FULL = 19


def date_formats(text, formats):
    """Sequentially try to parse using a set of formats. A format can be
    specified as a tuple that also specifies its granularity (e.g.
    ('%Y', YEAR) for year-only)."""
    if isinstance(text, (date, datetime)):
        return text.isoformat()
    if not isinstance(text, str):
        return text
    for fmt in formats:
        length = FULL
        try:
            fmt, length = fmt
        except (ValueError, TypeError):
            pass
        try:
            dt = datetime.strptime(text, fmt)
            return dt.isoformat()[:length]
        except (ValueError, TypeError):
            pass
    return text


def _parse_date_part(value):
    try:
        value = int(value)
        if value > 0:
            return value
    except (TypeError, ValueError):
        return None


def date_parts(year, month, day):
    """Compose a date string from a set of (year, month, day) components. If the day
    or month are empty, a prefix is generated."""
    year = _parse_date_part(year)
    if year is not None:
        text = str(year)
        month = _parse_date_part(month)
        if month is not None and month <= 12:
            text = f"{text}-{month:02}"
            day = _parse_date_part(day)
            if day is not None and day <= 31:
                text = f"{text}-{day:02}"
        return text


def is_empty(text):
    if text is None:
        return True
    if isinstance(text, str):
        text = text.strip()
        return len(text) == 0
    return False


def remove_namespace(doc):
    """Remove namespace in the passed document in place."""
    for elem in doc.getiterator():
        elem.tag = etree.QName(elem).localname
    etree.cleanup_namespaces(doc)
    return doc


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
