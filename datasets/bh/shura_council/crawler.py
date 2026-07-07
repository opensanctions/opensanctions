import json
import os
import re
from typing import Any

from normality import squash_spaces
from zavod.entity import Entity
from zavod.stateful.positions import (
    OccupancyStatus,
    PositionCategorisation,
    categorise,
)

from zavod import Context
from zavod import helpers as h

# The council site is a single-page app backed by a JSON API gated by a static
# key shipped verbatim in its public JavaScript bundle; requests without it are
# rejected. The key is not secret but is kept out of source and supplied via the
# environment.
BASE_URL = "https://shura.bh/shura/api/shura-external"
API_KEY = os.environ.get("OPENSANCTIONS_BH_SHURA_API_KEY")

# In place of a missing date the API returns a "1000-01-01" sentinel or the
# literal string "Invalid date".
NULL_DATE_PREFIX = "1000-"
INVALID_DATE = "Invalid date"
ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# The council-service flag drives whether a current-term member is marked as
# still in office; enumerate its known values so a new one raises a signal.
KNOWN_STATUSES = {"Current MP", "Previous MP"}

# Fields on each record type that are out of scope for this membership dataset.
# Anything not listed here (or consumed by `.pop()`) trips `audit_data`.
TERM_IGNORE = [
    "LTOrder",
    "LegislativeTermArabicName",
    "LegislativeTermEnglishName",
    "ShowInWebsite",
]
PERIOD_IGNORE = [
    "CPCurrent",
    "CPOrder",
    "ConveningPeriodArabicName",
    "ConveningPeriodEnglishName",
    "Description",
    "LegTermId",
    "ShowInWebsite",
]
RECORD_IGNORE = ["ID", "ConveningPeriodId", "UserOrder", "UserPosition_LOOKUP"]
# The `UserPosition_LOOKUP` (committee chair, deputy chairman, council president,
# etc.) and the CV/photo fields are out of scope for this membership dataset.
USER_INFO_IGNORE = [
    "ID",
    "FileDeleted",
    "FileURL",
    "UserBio",
    "UserCVFile",
    "UserCV_JSON",
    "UserCV_JSON_EN",
    "UserGroupId",
    "UserMiddlePhotoForWebsite",
    "UserOrder",
    "UserPhoto",
    "UserTitle_LOOKUP",
]

# Term descriptions give the start and end as Gregorian dates written with
# Arabic month names and the era marker "م" (Hijri dates use "هـ"), so anchoring
# on "م" picks out only the Gregorian ones. The dataset `dates` config maps the
# month names.
GREGORIAN_DATE_RE = re.compile(r"(\d{1,2}\s+\S+\s+\d{4})\s*م")


def fetch_api(
    context: Context, endpoint: str, body: dict[str, Any], cache_days: int
) -> Any:
    """POST to a Shura Council API endpoint and return its `data` list.

    Every endpoint wraps its rows in a paginated envelope. We request a page
    large enough to hold the whole result and fail loudly if the source ever
    grows beyond one page, rather than silently dropping the overflow.
    """
    assert API_KEY is not None, "OPENSANCTIONS_BH_SHURA_API_KEY not set."
    payload = context.fetch_json(
        f"{BASE_URL}/{endpoint}",
        method="POST",
        headers={"API-KEY": API_KEY, "Content-Type": "application/json"},
        data=json.dumps(body),
        cache_days=cache_days,
    )
    if payload["result"] != 0:
        raise RuntimeError(f"API error for {endpoint}: {payload!r}")
    response = payload["response"]
    pagination = response["pagination"]
    assert pagination["lastPage"] == 1, (endpoint, pagination)
    return response["data"]


def real_date(context: Context, value: str | None) -> str | None:
    """Return the ISO date part of an API timestamp, or None for its sentinels.

    Missing dates arrive as the "1000-01-01" sentinel or the literal string
    "Invalid date"; any other unparseable value is unexpected and warned about
    rather than silently dropped.
    """
    if value is None or value.startswith(NULL_DATE_PREFIX) or value == INVALID_DATE:
        return None
    date = value[:10]
    if ISO_DATE_RE.match(date):
        return date
    context.log.warning("Unparseable API date", value=value)
    return None


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
    is_current: bool,
    start_date: str | None,
    end_date: str | None,
    categorisation: PositionCategorisation,
    position: Entity,
) -> None:
    person = context.make("Person")
    person.id = context.make_slug("person", str(user_id))
    person.add("name", info.pop("UserEnglishName"), lang="eng")
    person.add("name", info.pop("UserArabicName"), lang="ara")
    gender = info.pop("UserGender_LOOKUP")
    if gender is not None:
        person.add("gender", gender["enLabel"])
    # Article 53 of the 2002 Constitution requires Consultative Council members
    # to hold Bahraini citizenship.
    person.add("citizenship", "bh")

    status_label = info.pop("UserStatus_LOOKUP")["enLabel"]
    if status_label not in KNOWN_STATUSES:
        context.log.warning(
            "Unknown member status", status=status_label, person=person.id
        )
    context.audit_data(info, ignore=USER_INFO_IGNORE)

    # A member who left during the ongoing term has no resignation date, so fall
    # back to the council's current-service flag to avoid marking them as still
    # in office.
    status = None
    if is_current and end_date is None and status_label != "Current MP":
        status = OccupancyStatus.ENDED

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
    is_current = term.pop("LTCurrent") == 1
    term_end = parse_term_end(context, term.pop("Description"))
    term_id = term.pop("ID")
    periods = term.pop("ConveningPeriods")
    context.audit_data(term, ignore=TERM_IGNORE)
    if not is_current and term_end is None:
        raise RuntimeError(f"Past term {term_id} has no parseable end date")

    members: dict[int, dict[str, Any]] = {}
    for period in periods:
        period_id = period.pop("ID")
        context.audit_data(period, ignore=PERIOD_IGNORE)
        body = {"page": 1, "size": 1000, "ConveningPeriodId": period_id}
        for record in fetch_api(context, "council-members-cp-id", body, 7):
            user_id = record.pop("UserId")
            infos = record.pop("UserInfo")
            assert len(infos) == 1, (user_id, len(infos))
            start = real_date(context, record.pop("DesignationDate"))
            resignation = real_date(context, record.pop("ResignationDate"))
            context.audit_data(record, ignore=RECORD_IGNORE)
            member = members.setdefault(
                user_id, {"info": infos[0], "start": None, "resignation": None}
            )
            if start is not None and (
                member["start"] is None or start < member["start"]
            ):
                member["start"] = start
            if resignation is not None:
                member["resignation"] = resignation

    for user_id, member in members.items():
        crawl_member(
            context,
            user_id,
            member["info"],
            is_current=is_current,
            start_date=member["start"],
            end_date=member["resignation"] or term_end,
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

    for term in fetch_api(context, "list-lts-with-cps", {"page": 1, "size": 1000}, 1):
        crawl_term(context, position, categorisation, term)
