import json
import re
from typing import Any

from normality import squash_spaces

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import (
    OccupancyStatus,
    PositionCategorisation,
    categorise,
)

# The council site is a single-page app backed by a JSON API gated by a static
# key shipped verbatim in its public JavaScript bundle; requests without it are
# rejected.
BASE_URL = "https://shura.bh/shura/api/shura-external"
API_KEY = "CB4E1B37-EECCA601-972C6B68-189AB6FE-9A771D24-0FDEBFB1-A4D4C063-DB6F152A"
HEADERS = {"API-KEY": API_KEY, "Content-Type": "application/json"}

# In place of a missing date the API returns a "1000-01-01" sentinel or the
# literal string "Invalid date".
NULL_DATE_PREFIX = "1000-"
ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Term descriptions give the start and end as Gregorian dates written with
# Arabic month names and the era marker "م" (Hijri dates use "هـ"), so anchoring
# on "م" picks out only the Gregorian ones. The dataset `dates` config maps the
# month names.
GREGORIAN_DATE_RE = re.compile(r"(\d{1,2}\s+\S+\s+\d{4})\s*م")


def fetch_api(
    context: Context, endpoint: str, body: dict[str, Any], cache_days: int
) -> Any:
    """POST to a Shura Council API endpoint and return its `response` payload."""
    data = context.fetch_json(
        f"{BASE_URL}/{endpoint}",
        method="POST",
        headers=HEADERS,
        data=json.dumps(body),
        cache_days=cache_days,
    )
    if data["result"] != 0:
        raise RuntimeError(f"API error for {endpoint}: {data!r}")
    return data["response"]


def real_date(value: str | None) -> str | None:
    """Return the ISO date part of an API timestamp, or None for its sentinels."""
    if value is None or value.startswith(NULL_DATE_PREFIX):
        return None
    date = value[:10]
    return date if ISO_DATE_RE.match(date) else None


def parse_term_end(context: Context, description: str | None) -> str | None:
    """Return a term's Gregorian end date, or None for the current term.

    The description embeds the term's start and end as Arabic-month Gregorian
    strings; the later of the two is the end. The current term has no dates yet.
    """
    if description is None:
        return None
    dates: list[str] = []
    for raw in GREGORIAN_DATE_RE.findall(squash_spaces(description)):
        dates.extend(h.extract_date(context.dataset, raw, fallback_to_original=False))
    return max(dates) if dates else None


def crawl_member(
    context: Context,
    user_id: int,
    info: dict[str, Any],
    status: OccupancyStatus | None,
    end_date: str | None,
    start_date: str | None,
    categorisation: PositionCategorisation,
    position: Entity,
) -> None:
    person = context.make("Person")
    person.id = context.make_slug("person", str(user_id))
    person.add("name", info["UserEnglishName"], lang="eng")
    person.add("name", info["UserArabicName"], lang="ara")
    gender = info.get("UserGender_LOOKUP")
    if gender is not None:
        person.add("gender", gender["enLabel"])
    # Article 53 of the 2002 Constitution requires Consultative Council members
    # to hold Bahraini citizenship.
    person.add("citizenship", "bh")

    occupancy = h.make_occupancy(
        context,
        person,
        position=position,
        start_date=start_date,
        end_date=end_date,
        status=status,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    context.emit(occupancy)
    context.emit(person)


def crawl_term(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    term: dict[str, Any],
) -> None:
    """Emit a person and occupancy for each member of one legislative term.

    A member recurs across the term's convening periods, so the roster is
    collapsed per member: the earliest designation date starts the occupancy and
    an individual resignation, or otherwise the term end, closes it.
    """
    is_current = term["LTCurrent"] == 1
    term_end = parse_term_end(context, term["Description"])
    if not is_current and term_end is None:
        raise RuntimeError(f"Past term {term['ID']} has no parseable end date")

    members: dict[int, dict[str, Any]] = {}
    for period in term["ConveningPeriods"]:
        body = {"page": 1, "size": 1000, "ConveningPeriodId": period["ID"]}
        roster = fetch_api(context, "council-members-cp-id", body, 7)
        for record in roster["data"]:
            member = members.setdefault(
                record["UserId"],
                {"info": record["UserInfo"][0], "start": None, "resignation": None},
            )
            start = real_date(record["DesignationDate"])
            if start is not None and (
                member["start"] is None or start < member["start"]
            ):
                member["start"] = start
            resignation = real_date(record["ResignationDate"])
            if resignation is not None:
                member["resignation"] = resignation

    for user_id, member in members.items():
        info = member["info"]
        end_date = member["resignation"] or term_end
        # A member who left during the ongoing term has no resignation date, so
        # fall back to the council's current-service flag to avoid marking them
        # as still in office.
        still_serving = info["UserStatus_LOOKUP"]["enLabel"] == "Current MP"
        status = None
        if is_current and end_date is None and not still_serving:
            status = OccupancyStatus.ENDED

        crawl_member(
            context,
            user_id,
            info,
            status=status,
            start_date=member["start"],
            end_date=end_date,
            categorisation=categorisation,
            position=position,
        )


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Consultative Council of Bahrain",
        country="bh",
        wikidata_id="Q21328598",
        topics=["gov.national", "gov.legislative"],
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    terms = fetch_api(context, "list-lts-with-cps", {"page": 1, "size": 1000}, 1)
    for term in terms["data"]:
        crawl_term(context, position, categorisation, term)
