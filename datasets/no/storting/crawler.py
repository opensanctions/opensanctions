import re
from datetime import datetime, timedelta, date, timezone
from typing import Any

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise, EXTENDED_AFTER_OFFICE

CUTOFF = datetime.now() - EXTENDED_AFTER_OFFICE


def parse_ms_date(ms_date: str | None) -> date | None:
    """
    Converts Microsoft JSON date format to a Python date object.

    Format: /Date(milliseconds±offset)/
    Example: /Date(1445301615000-0700)/

    The milliseconds are a UTC timestamp (milliseconds since Unix epoch).
    The offset indicates what the LOCAL time/date was in that timezone.

    Key insight from Stack Overflow
    (https://stackoverflow.com/questions/33224540/use-json-net-to-parse-json-date-of-format-dateepochtime-offset):
    - /Date(1445301615000-0700)/ means: "UTC time 2015-10-20T00:40:15Z,
      which in the -0700 timezone was 2015-10-19T17:40:15"
    - The offset doesn't modify the timestamp; it's metadata about local time

    We use naive (non-timezone-aware) dates everywhere in our data model and interpret based
    on context. For dates of birth, that means local time. So to get local time, we:
    1. Parse the UTC timestamp
    2. Apply the offset to get the local datetime
    3. Extract the date from that local datetime

    If no offset is provided, we use UTC date.

    Args:
        ms_date: Date string like /Date(1445301615000-0700)/ or None

    Returns:
        The local date in the specified timezone, or None if input is None

    Raises:
        ValueError: If the date string doesn't match expected format
    """
    if ms_date is None:
        return None

    match = re.match(r"/Date\((-?\d+)([+-]\d{4})?\)/", ms_date)
    if not match:
        raise ValueError(f"Invalid MS date format: {ms_date}")
    ms = int(match.group(1))
    tz_str = match.group(2)
    # Parse the UTC timestamp
    dt_utc = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    # If there's an offset, convert to that local timezone to get the local date
    if tz_str:
        sign = 1 if tz_str[0] == "+" else -1
        hours = int(tz_str[1:3])
        minutes = int(tz_str[3:5])
        local_tz = timezone(timedelta(hours=sign * hours, minutes=sign * minutes))
        dt_local = dt_utc.astimezone(local_tz)
        return dt_local.date()
    else:
        # No offset provided, return UTC date
        return dt_utc.date()


def translate_keys(context: Context, member: dict[str, Any]) -> dict[str, Any]:
    # Translate top-level keys
    translated = {context.lookup_value("keys", k) or k: v for k, v in member.items()}
    # Translate all nested dicts at level 2
    for k, v in translated.items():
        if isinstance(v, dict):
            translated[k] = {
                context.lookup_value("keys", nk) or nk: nv for nk, nv in v.items()
            }
    return translated


def get_latest_terms(context: Context) -> list[str]:
    data = context.fetch_json(
        "https://data.stortinget.no/eksport/stortingsperioder?format=json", cache_days=3
    )
    periods = data.get("stortingsperioder_liste", [])
    term_ids = []
    for p in periods:
        start_year_str = p["id"].split("-")[0]
        start_year = int(start_year_str)
        if start_year >= CUTOFF.year:
            term_ids.append(p["id"])
    # Sort newest first
    term_ids.sort(reverse=True)
    return term_ids


def crawl_item(context: Context, item: dict[str, Any], term: str) -> None:
    id = item.pop("id")
    first_name = item.pop("first_name")
    last_name = item.pop("last_name")

    entity = context.make("Person")
    entity.id = context.make_id(first_name, last_name, id)
    h.apply_name(entity, first_name=first_name, last_name=last_name)
    h.apply_date(entity, "birthDate", parse_ms_date(item.pop("birth_date")))
    h.apply_date(entity, "deathDate", parse_ms_date(item.pop("death_date", None)))
    entity.add("political", item.pop("party").pop("name"))
    entity.add("citizenship", "no")
    entity.add("gender", context.lookup_value("gender", item.pop("gender")), lang="eng")
    if not entity.get("gender")[0]:
        context.log.warning(f"Unknown gender for {entity.id}")

    position = h.make_position(
        context,
        name="Member of the Parliament of Norway",
        wikidata_id="Q9045502",
        country="no",
        topics=["gov.legislative", "gov.national"],
        lang="eng",
    )
    categorisation = categorise(context, position, is_pep=True)
    if not categorisation.is_pep:
        return

    occupancy = h.make_occupancy(
        context,
        person=entity,
        position=position,
        start_date=term.split("-")[0],
        end_date=term.split("-")[1],
        categorisation=categorisation,
    )
    if occupancy is not None:
        context.emit(occupancy)
        context.emit(position)
        context.emit(entity)

    context.audit_data(
        item,
        ["constituency", "response_date_time", "versjon", "is_deputy"],
    )


def crawl(context: Context) -> None:
    terms = get_latest_terms(context)
    for term in terms:
        url = f"{context.data_url}?stortingsperiodeid={term}&format=json"
        data = context.fetch_json((url), cache_days=3)
        items = data.get("representanter_liste")
        for item in items:
            item = translate_keys(context, item)
            crawl_item(context, item, term)
