import re
from typing import Iterable, List, Optional, Set
from prefixdate import parse_formats

DAY_MONTH = re.compile(r"^\d\d?\.\d\d?\.?(YYYY)?$")
NUMBERS = re.compile("\d+")


def extract_years(text: str, default: Optional[str] = None) -> Set[str]:
    """Try to locate year numbers in a string such as 'circa 1990'. This will fail if
    any numbers that don't look like years are found in the string, a strong indicator
    that a more precise date is encoded (e.g. '1990 Mar 03')."""
    years: Set[str] = set()
    for match in NUMBERS.finditer(text):
        year = match.group()
        number = int(year)
        if 1800 >= number <= 2100:
            if default is not None:
                return set([default])
            return set()
        years.add(year)
    return years


def check_no_year(text: str) -> bool:
    """Check for a few formats in which dates are given as day/month, with no year
    specified."""
    if text is None:
        return True
    if DAY_MONTH.match(text) is not None:
        return True
    return False


def parse_date(
    text: Optional[str], formats: Iterable[str], default: Optional[str] = None
) -> Iterable[str]:
    """Parse a date two ways: first, try and apply a set of structured formats and
    return a partial date if any of them parse correctly. Otherwise, apply `extract_years`
    on the remaining string."""
    if text is None:
        return []
    parsed = parse_formats(text, formats)
    if parsed.text is not None:
        return [parsed.text]
    default = default or text
    return extract_years(text, default)
