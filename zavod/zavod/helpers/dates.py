import re
from functools import lru_cache
from normality import stringify
from prefixdate import parse_formats
from rigour.dates import ended_before
from datetime import datetime, date, timedelta, UTC
from typing import TYPE_CHECKING
from collections.abc import Iterable
from followthemoney import registry

from zavod.logs import get_logger
from zavod.entity import Entity
from zavod.meta.dataset import Dataset
from zavod.settings import RUN_TIME

if TYPE_CHECKING:
    from zavod.context import Context

log = get_logger(__name__)
NUMBERS = re.compile(r"\b\d+\b")
# We always want to accept ISO prefix dates.
ALWAYS_FORMATS = ["%Y-%m-%d", "%Y-%m", "%Y"]
DateValue = str | date | datetime | None
MAX_ENFORCEMENT_DAYS = 365 * 5

__all__ = [
    "extract_date",
    "parse_formats",
    "extract_years",
    "apply_date",
    "apply_dates",
    "replace_months",
    "within_max_age",
]


def extract_years(text: str) -> list[str]:
    """Try to locate year numbers in a string such as 'circa 1990'. This will fail if
    any numbers that don't look like years are found in the string, a strong indicator
    that a more precise date is encoded (e.g. '1990 Mar 03').

    This is bounded to years between 1800 and 2100.

    Args:
        text: a string to extract years from.

    Returns:
        a set of year strings.
    """
    years: set[str] = set()
    for match in NUMBERS.finditer(text):
        year = match.group()
        number = int(year)
        if number < 1800 or number > 2100:
            continue
        years.add(year)
    return list(years)


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
    dataset: Dataset,
    text: DateValue,
    formats: tuple[str] | None = None,
    fallback_to_original: bool = True,
) -> list[str]:
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
            text = text.astimezone(UTC)
        iso = text.date().isoformat()
        return [iso]
    elif isinstance(text, str):
        text = text.strip()

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
    if fallback_to_original:
        return [text]
    raise ValueError(f"Invalid date: {text}")


def apply_date(
    entity: Entity,
    prop: str,
    text: DateValue,
    formats: tuple[str] | None = None,
    original_value: str | None = None,
) -> None:
    """Apply a date value to an entity, parsing it if necessary and cleaning it up.

    Uses the `dates` configuration of the dataset to parse the date.

    Args:
        entity: The entity to which the date will be applied.
        prop: The property to which the date will be applied.
        text: The date value to be applied.
        formats: A list of date formats to use for parsing, overriding dataset defaults.
        original_value: If provided, recorded as the entity's original value for
            this property instead of ``text``. Use when ``text`` has already been
            transformed.
    """
    prop_ = entity.schema.get(prop)
    if prop_ is None or prop_.type != registry.date:
        log.warning(f"Property is not a date: {prop}", text=text)
        return

    if not isinstance(text, str):
        text = stringify(text)
    if text is None:
        return None

    if original_value is None:
        original_value = text
    dates = extract_date(entity.dataset, text, formats=formats)
    return entity.add(prop_, dates, original_value=original_value)


def apply_dates(entity: Entity, prop: str, texts: Iterable[DateValue]) -> None:
    """Apply a list of date values to an entity, parsing them if necessary and cleaning them up.

    Args:
        entity: The entity to which the date will be applied.
        prop: The property to which the date will be applied.
        texts: The iterable of date values to be applied.
    """
    for text in texts:
        apply_date(entity, prop, text)


def backdate(date: datetime, delta: timedelta) -> str:
    """Return a partial ISO8601 date string backdated by the number of days provided"""
    dt = date - delta
    return dt.isoformat()[:10]


def within_max_age(
    context: "Context",
    date: datetime | str,
    max_age_days: int = MAX_ENFORCEMENT_DAYS,
) -> bool:
    """
    Check if a the given date is within a specified maximum age, defaulting to `MAX_ENFORCEMENT_DAYS`.

    This is useful for filtering out all but the most recent items, e.g. sanctions announcements
    or enforcement actions.

    Args:
        context: The runner context with dataset metadata.
        date: The date to check.
        max_age_days: The maximum allowable age in days, if different from the default.
    """
    if isinstance(date, str):
        date = date.strip()
    cleaned_date = extract_date(context.dataset, date, fallback_to_original=False)[0]
    return not ended_before(cleaned_date, RUN_TIME - timedelta(days=max_age_days))
