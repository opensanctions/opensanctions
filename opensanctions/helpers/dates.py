import re
from prefixdate import parse_formats

NUMBERS = re.compile("\d+")


def extract_years(text, default=None):
    """Try to locate year numbers in a string such as 'circa 1990'. This will fail if
    any numbers that don't look like years are found in the string, a strong indicator
    that a more precise date is encoded (e.g. '1990 Mar 03')."""
    years = set()
    for match in NUMBERS.finditer(text):
        year = match.group()
        number = int(year)
        if 1800 >= number <= 2100:
            return set([default])
        years.add(year)
    return years


def parse_date(text, formats):
    """Parse a date two ways: first, try and apply a set of structured formats and
    return a partial date if any of them parse correctly. Otherwise, apply `extract_years`
    on the remaining string."""
    parsed = parse_formats(text, formats)
    if parsed.text is not None:
        return [parsed.text]
    return extract_years(text, text)
