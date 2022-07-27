import re
import orjson
import logging
import Levenshtein
from functools import cache
from banal import ensure_list
from datetime import datetime
from itertools import combinations
from collections import defaultdict
from typing import IO, Any, Dict, List, Optional, Tuple
from normality import latinize_text, slugify

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


@cache
def pick_name(names: Tuple[str], all_names: Tuple[str]) -> Optional[str]:
    candidates: List[str] = []
    for name in all_names:
        candidates.append(name)
        latin = latinize_text(name)
        if latin is not None:
            candidates.append(latin.title())

    scores: Dict[str, int] = defaultdict(int)
    for pair in combinations(candidates, 2):
        left, right = sorted(pair)
        dist = Levenshtein.distance(left[:128], right[:128])
        scores[left] += dist
        scores[right] += dist

    for cand, _ in sorted(scores.items(), key=lambda x: x[1]):
        if cand in names:
            return cand
    return None


def json_default(obj: Any) -> Any:
    if isinstance(obj, (tuple, set)):
        return list(obj)
    raise TypeError


def write_json(data: Dict[str, Any], fh: IO[bytes]) -> None:
    """Write a JSON object to the given open file handle."""
    opt = orjson.OPT_APPEND_NEWLINE | orjson.OPT_NON_STR_KEYS
    fh.write(orjson.dumps(data, option=opt, default=json_default))
