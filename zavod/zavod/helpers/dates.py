import re
import warnings
from functools import lru_cache
from prefixdate import parse_formats
from datetime import datetime, date, timezone
from typing import Tuple, Union, Iterable, Set, Optional, List
from followthemoney.types import registry

from zavod.logs import get_logger
from zavod.entity import Entity
from zavod.meta.dataset import Dataset

log = get_logger(__name__)
NUMBERS = re.compile(r"\d+")
# We always want to accept ISO prefix dates.
ALWAYS_FORMATS = ["%Y-%m-%d", "%Y-%m", "%Y"]
DateValue = Union[str, date, datetime, None]

__all__ = [
    "extract_date",
    "check_no_year",
    "parse_formats",
    "extract_years",
    "apply_date",
    "apply_dates",
    "replace_months",
]


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


def replace_months(dataset: Dataset, text: str) -> str:
    """Re-write month names to the latin form to get a date string ready for parsing.

    Args:
        dataset: The dataset which contains a date format specification.
        text: The string inside of which month names will be replaced.

    Returns:
        A string in which month names are normalized.
    """
    spec = dataset.dates
    if spec.months_re is None:
        return text
    return spec.months_re.sub(lambda m: spec.mappings[m.group().lower()], text)


@lru_cache(maxsize=5000)
def extract_date(
    dataset: Dataset, text: DateValue, formats: Optional[Tuple[str]] = None
) -> List[str]:
    """
    Extract a date from the provided text using predefined `formats` in the metadata.
    If the text doesn't match any format, returns the original text.
    """
    if text is None:
        return []
    if isinstance(text, date):
        return [text.isoformat()]
    elif isinstance(text, datetime):
        if text.tzinfo is not None:
            text = text.astimezone(timezone.utc)
        iso = text.date().isoformat()
        return [iso]

    replaced_text = replace_months(dataset, text)
    dataset_formats_ = dataset.dates.formats + ALWAYS_FORMATS
    formats_ = dataset_formats_ if formats is None else list(formats)
    parsed = parse_formats(replaced_text, formats_)
    if parsed.text is not None:
        return [parsed.text]
    if dataset.dates.year_only:
        years = extract_years(text)
        if len(years):
            return years
    return [text]


def apply_date(
    entity: Entity, prop: str, text: DateValue, formats: Optional[Tuple[str]] = None
) -> None:
    """Apply a date value to an entity, parsing it if necessary and cleaning it up.

    Uses the `dates` configuration of the dataset to parse the date.

    Args:
        entity: The entity to which the date will be applied.
        prop: The property to which the date will be applied.
        text: The date value to be applied.
        formats: A list of date formats to use for parsing, overriding dataset defaults.
    """
    prop_ = entity.schema.get(prop)
    if prop_ is None or prop_.type != registry.date:
        log.warning("Property is not a date: %s" % prop, text=text)
        return

    if text is None:
        return None
    if isinstance(text, datetime) or isinstance(text, date):
        original = str(text)
    else:
        original = text

    dates = extract_date(entity.dataset, text, formats=formats)
    return entity.add(prop_, dates, original_value=original)


def apply_dates(entity: Entity, prop: str, texts: Iterable[DateValue]) -> None:
    """Apply a list of date values to an entity, parsing them if necessary and cleaning them up.

    Args:
        entity: The entity to which the date will be applied.
        prop: The property to which the date will be applied.
        texts: The iterable of date values to be applied.
    """
    for text in texts:
        apply_date(entity, prop, text)
