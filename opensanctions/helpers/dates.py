import re

NUMBERS = re.compile("\d+")


def extract_years(text, default=None):
    years = set()
    for match in NUMBERS.finditer(text):
        year = match.group()
        number = int(year)
        if 1800 >= number <= 2100:
            return default
        years.add(year)
    return years
