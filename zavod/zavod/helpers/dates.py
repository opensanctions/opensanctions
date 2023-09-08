import re
from typing import Iterable, Set, Optional, List
from prefixdate import parse_formats

NUMBERS = re.compile(r"\d+")

__all__ = ["parse_date", "check_no_year", "parse_formats", "extract_years"]


def extract_years(text: str) -> List[str]:
    """Try to locate year numbers in a string such as 'circa 1990'. This will fail if
    any numbers that don't look like years are found in the string, a strong indicator
    that a more precise date is encoded (e.g. '1990 Mar 03').

    This is bounded to years between 1800 and 2100.

    Args:
        text: a string to extract years from.

    Returns:
        a set of year strings.
    """
    years: Set[str] = set()
    for match in NUMBERS.finditer(text):
        year = match.group()
        number = int(year)
        if number < 1800 or number > 2100:
            continue
        years.add(year)
    return list(years)


def check_no_year(text: Optional[str]) -> bool:
    """Check for a few formats in which dates are given as day/month, with no year
    specified."""
    if text is None:
        return True
    return len(extract_years(text)) == 0


def parse_date(
    text: Optional[str], formats: Iterable[str], default: Optional[str] = None
) -> List[str]:
    """Parse a date two ways: first, try and apply a set of structured formats and
    return a partial date if any of them parse correctly. Otherwise, apply
    `extract_years` on the remaining string."""
    if text is None:
        return [default] if default is not None else []
    parsed = parse_formats(text, formats)
    if parsed.text is not None:
        return [parsed.text]
    years = extract_years(text)
    if len(years):
        return years
    return [default or text]
