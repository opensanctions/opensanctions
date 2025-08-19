import re
from banal import is_listish, ensure_list
from typing import Optional, List, Union, Iterable
from normality import squash_spaces

PREFIX_ = r"INTERPOL-UN\s*Security\s*Council\s*Special\s*Notice\s*web\s*link:?"
PREFIX = re.compile(PREFIX_, re.IGNORECASE)

INTERPOL_URL_ = r"https?:\/\/www\.interpol\.int\/[^ ]*(\s\d+)?"
INTERPOL_URL = re.compile(INTERPOL_URL_, re.IGNORECASE)
BRACKETED = re.compile(r"\(.*\)")
TRAILING_WHITESPACE = re.compile(r"\s+$", re.MULTILINE)


def clean_note(text: Union[Optional[str], List[Optional[str]]]) -> List[str]:
    """Remove a set of specific text sections from notes supplied by sanctions data
    publishers. These include cross-references to the Security Council web site and
    the Interpol web site.

    Args:
        text: The note text from source

    Returns:
        A cleaned version of the text.
    """
    out: List[str] = []
    if text is None:
        return out
    if is_listish(text):
        for t in text:
            out.extend(clean_note(t))
        return out
    if isinstance(text, str):
        text = PREFIX.sub(" ", text)
        text = INTERPOL_URL.sub(" ", text)
        text = squash_spaces(text)
        if len(text) == 0:
            return out
        return [text]
    return out


def multi_split(
    text: Optional[Union[str, Iterable[Optional[str]]]], splitters: Iterable[str]
) -> List[str]:
    """Sequentially attempt to split a text based on an array of splitting criteria.
    This is useful for strings where multiple separators are used to separate values,
    e.g.: `test,other/misc`. A special case of this is itemised lists like `a) test
    b) other c) misc` which sanction-makers seem to love.

    Args:
        text: A text or list of texts to be split up further.
        splitters: A sequence of text splitting criteria to be applied to the text.

    Returns:
        Fully subdivided text snippets.
    """
    if text is None:
        return []
    fragments = ensure_list(text)
    for splitter in splitters:
        out: List[Optional[str]] = []
        for fragment in fragments:
            if fragment is None:
                continue
            for frag in fragment.split(splitter):
                frag = frag.strip()
                if len(frag):
                    out.append(frag)
        fragments = out
    return [f for f in fragments if f is not None]


def is_empty(text: Optional[str]) -> bool:
    """Check if the given text is empty: it can either be null, or
    the stripped version of the string could have 0 length.

    Args:
        text: Text to be checked

    Returns:
        Whether the text is empty or not.
    """
    if text is None:
        return True
    if isinstance(text, str):
        text = text.strip()
        return len(text) == 0
    return False


def remove_bracketed(text: Optional[str]) -> Optional[str]:
    """Helps to deal with property values where additional info has been supplied in
    brackets that makes it harder to parse the value. Examples:

    - Russia (former USSR)
    - 1977 (as Muhammad Da'ud Salman)

    It's probably not useful in all of these cases to try and parse and derive meaning
    from the bracketed bit, so we'll just discard it.

    Args:
        text: Text with sub-text in brackets

    Returns:
        Text that was not in brackets.
    """
    if text is None:
        return None
    return BRACKETED.sub(" ", text)


def strip_trailing_whitespace(text: str) -> str:
    """Strip trailing whitespace, preserving newlines."""
    return TRAILING_WHITESPACE.sub("", text)
