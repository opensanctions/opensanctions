from typing import Dict, List
from collections import defaultdict
from rigour.langs import iso_639_alpha3

from zavod import Context, helpers as h
from zavod.entity import Entity
from zavod.logic.pep import OccupancyStatus, categorise
from zavod.shed.zyte_api import fetch_json

DATE_FORMATS = ["%B %d, %Y"]
CACHE_SHORT = 1
CACHE_LONG = 30
STATUSES: Dict[int, int] = defaultdict(int)
HEADERS = [{"name": "Accept", "value": "application/json"}]
PEPS = set()


def crawl_position(
    context: Context,
    person: Entity,
    position: Entity,
    birth_date: List[str],
    death_date: List[str],
    tenure,
    status=None,
):
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        no_end_implies_current=tenure.pop("isCurrentMk"),
        # These are already ISO
        start_date=tenure.pop("FromDate"),
        end_date=tenure.pop("ToDate"),
        birth_date=birth_date[0] if birth_date else None,
        death_date=death_date[0] if death_date else None,
        status=status,
    )
    if not occupancy:
        return
    context.emit(person, target=True)
    context.emit(occupancy)
    PEPS.add(person.id)


def crawl_position_no_tenure(
    context: Context, person: Entity, position: Entity, knesset, is_current: bool
):
    if knesset["KnessetNumber"] < 17:
        return
    if knesset["IsGov"] and not is_current:
        # https://main.knesset.gov.il/en/mk/Apps/mk/mk-positions/866
        # has bogus knesset 25 membership
        return
    if is_current and knesset["IsKnessetCurrent"]:
        status = OccupancyStatus.CURRENT
    else:
        status = OccupancyStatus.ENDED

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        status=status,
    )
    if not occupancy:
        return

    parts = [
        person.id,
        position.id,
        knesset["KnessetNumber"],
    ]
    occupancy.id = context.make_id(*parts)
    occupancy.add("description", f"Knesset {knesset['KnessetNumber']}")
    context.emit(person, target=True)
    context.emit(occupancy)
    PEPS.add(person.id)


def crawl_positions(
    context: Context,
    person: Entity,
    member_id,
    birth_date,
    death_date,
    is_current: bool,
):
    position = h.make_position(
        context,
        "Knesset Member",
        country="il",
        topics=["gov.national", "gov.legislative"],
    )
    categorisation = categorise(context, position, is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    url = f"https://knesset.gov.il/WebSiteApi/knessetapi/MKs/GetMkPositions?mkId={member_id}&languageKey=en"
    try:
        response = fetch_json(context, url, cache_days=CACHE_LONG, geolocation="IL")
    except Exception as err:
        context.log.exception("HTTP request error", url=url, err=str(err))
        STATUSES[str(err)] += 1
        return
    if response is None:
        # The GetMkPositions endpoint returns null for everyone at the moment but it's still
        # used by the website.
        url = f"https://knesset.gov.il/WebSiteApi/knessetapi/MKs/GetMkKnassot?mkId={member_id}"
        try:
            response = fetch_json(context, url, cache_days=CACHE_LONG, geolocation="IL")
        except Exception as err:
            context.log.exception("HTTP request error", url=url, err=str(err))
            STATUSES[str(err)] += 1
            return
        for knesset in response:
            crawl_position_no_tenure(context, person, position, knesset, is_current)
        return
    for row in response:
        for tenure in row.pop("Tenure"):
            crawl_position(context, person, position, birth_date, death_date, tenure)


def crawl_item(
    context: Context,
    member_id: int,
    name: str,
    is_current: bool,
    lang_iso_639_1: str,
):
    lang_iso_639_2 = iso_639_alpha3(lang_iso_639_1)
    url = f"https://knesset.gov.il/WebSiteApi/knessetapi/MKs/GetMkDetailsContent?mkId={member_id}&languageKey={lang_iso_639_1}"
    try:
        content = fetch_json(context, url, cache_days=CACHE_LONG, geolocation="IL")
    except Exception as err:
        context.log.exception("HTTP request error", url=url, err=str(err))
        STATUSES[str(err)] += 1
        return

    person = context.make("Person")
    person.id = context.make_slug(str(member_id))
    person.add("name", name, lang=lang_iso_639_2)
    person.add(
        "sourceUrl",
        f"https://main.knesset.gov.il/{lang_iso_639_1}/MK/APPS/mk/mk-personal-details/{member_id}",
    )
    if content:
        person.add("birthPlace", content.pop("PlaceOfBirth"), lang=lang_iso_639_2)

    if lang_iso_639_1 == "en":

        birth_date = None
        death_date = None
        if content:
            birth_date = h.parse_date(content.pop("DateOfBirth"), DATE_FORMATS)
            death_date = h.parse_date(content.pop("DeathDate"), DATE_FORMATS)
            person.add("birthDate", birth_date)
            person.add("deathDate", death_date)

        crawl_positions(context, person, member_id, birth_date, death_date, is_current)
    if lang_iso_639_1 == "he":
        if person.id in PEPS:
            context.emit(person, target=True)


def crawl(context: Context):
    members = fetch_json(
        context, context.data_url, cache_days=CACHE_SHORT, geolocation="IL"
    )
    for member in members:
        crawl_item(context, member["ID"], member["Name"], member["IsCurrent"], "en")

    url_heb = context.data_url.replace("languageKey=en", "languageKey=he")
    members_heb = fetch_json(context, url_heb, cache_days=CACHE_SHORT, geolocation="IL")
    for member in members_heb:
        crawl_item(context, member["ID"], member["Name"], member["IsCurrent"], "he")

    if STATUSES:
        raise RuntimeError("Error counts: %r" % STATUSES)
