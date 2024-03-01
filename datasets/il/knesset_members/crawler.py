from datetime import datetime
import json
from time import sleep
from languagecodes import iso_639_alpha3

from zavod import Context, helpers as h
from zavod.entity import Entity
from zavod.logic.pep import categorise

DATE_FORMATS = ["%B %d, %Y"]
CACHE_SHORT = 1
CACHE_LONG = 14


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

    positions_url = f"https://knesset.gov.il/WebSiteApi/knessetapi/MKs/GetMkPositions?mkId={member_id}&languageKey=en"
    for row in context.fetch_json(positions_url, cache_days=CACHE_LONG):
        for tenure in row.pop("Tenure"):
            crawl_position(context, person, position, tenure)


def crawl_item(context: Context, member_id: int, name: str, lang: str):
    lang3 = iso_639_alpha3(lang)
    content_url = f"https://knesset.gov.il/WebSiteApi/knessetapi/MKs/GetMkDetailsContent?mkId={member_id}&languageKey={lang}"
    content = context.fetch_json(content_url, cache_days=CACHE_LONG)

    person = context.make("Person")
    person.id = context.make_slug(member_id)
    person.add("name", name, lang=lang3)
    person.add(
        "sourceUrl",
        f"https://main.knesset.gov.il/{lang}/MK/APPS/mk/mk-personal-details/{member_id}",
    )
    if content:
        person.add("birthPlace", content.pop("PlaceOfBirth"), lang=lang3)

    if lang == "en":
        if content:
            person.add(
                "birthDate", h.parse_date(content.pop("DateOfBirth"), DATE_FORMATS)
            )
            person.add(
                "deathDate", h.parse_date(content.pop("DeathDate"), DATE_FORMATS)
            )

        crawl_positions(context, person, member_id)


def crawl(context: Context):
    for member in context.fetch_json(context.data_url, cache_days=CACHE_SHORT):
        crawl_item(context, member["ID"], member["Name"], "en")

    hebrew_url = context.data_url.replace("languageKey=en", "languageKey=he")
    for member in context.fetch_json(hebrew_url, cache_days=CACHE_SHORT):
        crawl_item(context, member["ID"], member["Name"], "he")
