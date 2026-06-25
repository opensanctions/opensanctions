import json
import re
from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import (
    OccupancyStatus,
    PositionCategorisation,
    categorise,
)

# The council website is a single-page app that talks to a JSON API gated by a
# static, publicly-shipped API key (it is embedded verbatim in the site's
# JavaScript bundle). The endpoints reject requests without it.
BASE_URL = "https://shura.bh/shura/api/shura-external"
API_KEY = "CB4E1B37-EECCA601-972C6B68-189AB6FE-9A771D24-0FDEBFB1-A4D4C063-DB6F152A"
HEADERS = {"API-KEY": API_KEY, "Content-Type": "application/json"}

# The API returns this sentinel date instead of null for missing
# designation/resignation dates.
NULL_DATE_PREFIX = "1000-"

# The legislative-term descriptions spell out the term's start and end dates as
# Arabic-month Gregorian strings, each suffixed with the era marker "م"
# (e.g. "... 14 ديسمبر 2002م إلى ... 27 يوليو 2006م"). Hijri dates in the same
# text use "هـ" instead, so anchoring on "م" selects only the Gregorian ones.
# The month names themselves are translated by the dataset `dates` config.
GREGORIAN_DATE_RE = re.compile(r"(\d{1,2}\s+\S+\s+\d{4})\s*م")
ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def fetch_api(
    context: Context, endpoint: str, body: dict[str, Any], cache_days: int
) -> Any:
    """POST to a Shura Council API endpoint and return the decoded `response`."""
    data = context.fetch_json(
        f"{BASE_URL}/{endpoint}",
        method="POST",
        headers=HEADERS,
        data=json.dumps(body),
        cache_days=cache_days,
    )
    if data.get("result") != 0:
        raise RuntimeError(f"API error for {endpoint}: {data!r}")
    return data["response"]


def parse_term_end(context: Context, description: str | None) -> str | None:
    """Extract a legislative term's Gregorian end date from its description.

    The description embeds the term's start and end dates as Arabic-month
    Gregorian strings; the later of the two is the end of the term. Returns
    None for the current term, whose description has no dates yet.
    """
    if description is None:
        return None
    text = description.replace("\xa0", " ").replace("\n", " ")
    dates: list[str] = []
    for raw in GREGORIAN_DATE_RE.findall(text):
        dates.extend(h.extract_date(context.dataset, raw, fallback_to_original=False))
    if not dates:
        return None
    return max(dates)


def real_date(value: str | None) -> str | None:
    """Return the date part of an API timestamp, or None.

    The API uses both a "1000-01-01" sentinel and the literal string
    "Invalid date" in place of a missing date.
    """
    if value is None or value.startswith(NULL_DATE_PREFIX):
        return None
    date = value[:10]
    if not ISO_DATE_RE.match(date):
        return None
    return date


def make_member(context: Context, user_id: int, info: dict[str, Any]) -> Entity:
    """Build the Person entity for a council member from a roster record."""
    person = context.make("Person")
    person.id = context.make_slug("person", str(user_id))
    person.add("name", info["UserEnglishName"], lang="eng")
    person.add("name", info["UserArabicName"], lang="ara")
    gender = info.get("UserGender_LOOKUP")
    if gender is not None:
        person.add("gender", gender["enLabel"])
    # The Constitution of Bahrain (2002), Article 53, requires a member of the
    # Consultative Council to hold Bahraini citizenship:
    # https://www.lloc.gov.bh/en/page/The%20Constitution%20of%20the%20Kingdom%20of%20Bahrain
    person.add("citizenship", "bh")
    return person


def crawl_term(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    term: dict[str, Any],
) -> None:
    """Emit a person and occupancy for each member of one legislative term.

    A member recurs across the term's convening periods, so the roster is
    collapsed by member first: the earliest real designation date is the start
    of the occupancy, the term end (or an individual resignation) is the end.
    Members are emitted once per term they served; statements merge downstream.
    """
    is_current = term["LTCurrent"] == 1
    term_end = parse_term_end(context, term["Description"])
    if not is_current and term_end is None:
        raise RuntimeError(
            f"Past legislative term {term['ID']} has no parseable end date: "
            f"{term['Description']!r}"
        )

    members: dict[int, dict[str, Any]] = {}
    for period in term["ConveningPeriods"]:
        body = {"page": 1, "size": 1000, "ConveningPeriodId": period["ID"]}
        roster = fetch_api(context, "council-members-cp-id", body, 7)
        for record in roster["data"]:
            user_id = record["UserId"]
            member = members.setdefault(
                user_id,
                {"info": record["UserInfo"][0], "start": None, "resignation": None},
            )
            designation = real_date(record["DesignationDate"])
            if designation is not None and (
                member["start"] is None or designation < member["start"]
            ):
                member["start"] = designation
            resignation = real_date(record["ResignationDate"])
            if resignation is not None:
                member["resignation"] = resignation

    for user_id, member in members.items():
        info = member["info"]
        end_date = member["resignation"] or term_end

        # The council reports whether a member is currently serving. A member who
        # left during the ongoing term has no recorded resignation date, so this
        # flag keeps us from marking a departed member's occupancy as current.
        still_serving = info["UserStatus_LOOKUP"]["enLabel"] == "Current MP"
        status = None
        if is_current and end_date is None and not still_serving:
            status = OccupancyStatus.ENDED

        person = make_member(context, user_id, info)
        occupancy = h.make_occupancy(
            context,
            person,
            position,
            start_date=member["start"],
            end_date=end_date,
            status=status,
            categorisation=categorisation,
        )
        if occupancy is None:
            continue
        context.emit(occupancy)
        context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Shura Council",
        country="bh",
        wikidata_id="Q21328598",
        topics=["gov.national", "gov.legislative"],
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    terms_data = fetch_api(context, "list-lts-with-cps", {"page": 1, "size": 1000}, 1)
    for term in terms_data["data"]:
        crawl_term(context, position, categorisation, term)
