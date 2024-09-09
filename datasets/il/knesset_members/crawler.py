from typing import Dict
from collections import defaultdict
from rigour.langs import iso_639_alpha3

from zavod import Context, helpers as h
from zavod.entity import Entity
from zavod.logic.pep import categorise
from zavod.shed.zyte_api import fetch_json

DATE_FORMATS = ["%B %d, %Y"]
CACHE_SHORT = 1
CACHE_LONG = 30
STATUSES: Dict[int, int] = defaultdict(int)
HEADERS = [{"name": "Accept", "value": "application/json"}]
PEPS = set()


def crawl_position(context: Context, person: Entity, position: Entity, tenure):
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        no_end_implies_current=tenure.pop("isCurrentMk"),
        # These are already ISO
        start_date=tenure.pop("FromDate"),
        end_date=tenure.pop("ToDate"),
    )
    if not occupancy:
        return
    context.emit(person, target=True)
    context.emit(occupancy)
    PEPS.add(person.id)


def crawl_positions(context: Context, person: Entity, member_id):
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
    for row in response:
        for tenure in row.pop("Tenure"):
            crawl_position(context, person, position, tenure)


def crawl_item(context: Context, member_id: int, name: str, lang_iso_639_1: str):
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
    print(name)
    person.add(
        "sourceUrl",
        f"https://main.knesset.gov.il/{lang_iso_639_1}/MK/APPS/mk/mk-personal-details/{member_id}",
    )
    if content:
        person.add("birthPlace", content.pop("PlaceOfBirth"), lang=lang_iso_639_2)

    if lang_iso_639_1 == "en":
        if content:
            person.add(
                "birthDate", h.parse_date(content.pop("DateOfBirth"), DATE_FORMATS)
            )
            person.add(
                "deathDate", h.parse_date(content.pop("DeathDate"), DATE_FORMATS)
            )

        crawl_positions(context, person, member_id)
    if lang_iso_639_1 == "he":
        if person.id in PEPS:
            context.emit(person, target=True)


def crawl(context: Context):
    members = fetch_json(
        context, context.data_url, cache_days=CACHE_SHORT, geolocation="IL"
    )
    for member in members:
        crawl_item(context, member["ID"], member["Name"], "en")

    url_heb = context.data_url.replace("languageKey=en", "languageKey=he")
    members_heb = fetch_json(context, url_heb, cache_days=CACHE_SHORT, geolocation="IL")
    for member in members_heb:
        crawl_item(context, member["ID"], member["Name"], "he")

    if STATUSES:
        raise RuntimeError("Error counts: %r" % STATUSES)
