import re
from datetime import date, datetime, timedelta

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise

DATA_URL = "https://data.stortinget.no/eksport/representanter?stortingsperiodeid={term}&format=json"
CUTOFF_YEAR = date.today().year - 20


def parse_ms_date(ms_date: str) -> str | None:
    """
    Converts /Date(1763485375798+0100)/ or /Date(-106963200000+0200)/
    to ISO format date string 'YYYY-MM-DD'.
    Returns None if input is None.
    """
    if not ms_date:
        return None
    # Extract milliseconds and optional timezone offset
    match = re.match(r"/Date\((?P<ms>-?\d+)(?P<tz>[+-]\d{4})?\)/", ms_date)
    if not match:
        raise ValueError(f"Invalid MS date format: {ms_date}")
    ms = int(match.group("ms"))
    tz = match.group("tz")
    # Convert milliseconds to UTC datetime
    dt = datetime.utcfromtimestamp(ms / 1000)
    # Apply timezone offset if present
    if tz:
        sign = 1 if tz[0] == "+" else -1
        hours_offset = int(tz[1:3])
        minutes_offset = int(tz[3:5])
        dt += timedelta(hours=hours_offset, minutes=minutes_offset) * sign
    return str(dt.date().isoformat())


def translate_keys(member, context) -> dict:
    # Translate top-level keys
    translated = {context.lookup_value("columns", k) or k: v for k, v in member.items()}
    # Translate all nested dicts at level 2
    for k, v in translated.items():
        if isinstance(v, dict):
            translated[k] = {
                context.lookup_value("columns", nk) or nk: nv for nk, nv in v.items()
            }
    return translated


def get_latest_terms(context) -> list[str]:
    data = context.fetch_json(
        "https://data.stortinget.no/eksport/stortingsperioder?format=json", cache_days=3
    )
    periods = data.get("stortingsperioder_liste", [])
    term_ids = []
    for p in periods:
        start_year_str = p["id"].split("-")[0]
        start_year = int(start_year_str)
        if start_year >= CUTOFF_YEAR:
            term_ids.append(p["id"])
    # Sort newest first
    term_ids.sort(reverse=True)
    return term_ids


def crawl_item(context, item: dict, term: str) -> None:
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
        data = context.fetch_json(DATA_URL.format(term=term), cache_days=3)
        items = data.get("representanter_liste")
        for item in items:
            item = translate_keys(item, context)
            crawl_item(context, item, term)
