import re
from banal import is_listish
from typing import Iterable, Optional, List, Set
from normality import collapse_spaces

PREFIX_ = "INTERPOL-UN\s*Security\s*Council\s*Special\s*Notice\s*web\s*link:?"
PREFIX = re.compile(PREFIX_, re.IGNORECASE)

INTERPOL_URL_ = "https?:\/\/www\.interpol\.int\/[^ ]*(\s\d+)?"
INTERPOL_URL = re.compile(INTERPOL_URL_, re.IGNORECASE)


def clean_note(text: Optional[str]) -> List[str]:
    out: List[str] = []
    if text is None:
        return out
    if is_listish(text):
        for t in text:
            out.extend(clean_note(t))
        return out
    text = PREFIX.sub(" ", text)
    text = INTERPOL_URL.sub(" ", text)
    text = collapse_spaces(text)
    if text is None:
        return out
    return [text]


def _position_date(dates: Iterable[Optional[str]]) -> Set[str]:
    cleaned: Set[str] = set()
    for date in dates:
        if date is not None:
            cleaned.add(date[:4])
    return cleaned


def make_position(
    main: str,
    comment: Optional[str],
    starts: Iterable[Optional[str]],
    ends: Iterable[Optional[str]],
    dates: Iterable[Optional[str]],
) -> str:
    position = main
    start = min(_position_date(starts), default="")
    end = min(_position_date(ends), default="")
    date_range = None
    if len(start) or len(end):
        date_range = f"{start}-{end}"
    dates_ = _position_date(dates)
    if date_range is None and len(dates_):
        date_range = ", ".join(sorted(dates_))

    bracketed = None
    if date_range and comment:
        bracketed = f"{comment}, {date_range}"
    else:
        bracketed = comment or date_range

    if bracketed:
        position = f"{position} ({bracketed})"
    return position
