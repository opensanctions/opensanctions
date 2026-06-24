from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# The endpoint returns the sitting convocation's deputies. Names come back in the locale
# selected by Accept-Language and are otherwise identical, so we fetch the Kazakh
# (default) and Russian variants and merge them by deputy id to keep both name forms.
PARAMS = {"ord": "last_name"}

IGNORE = [
    "phone",  # private contact detail
    "telegram_link",
    "twitter_link",
    "youtube_link",
    "instagram_link",
    "facebook_link",
    "deputy_type",  # 1 = single-mandate district, 2 = party-list
    "position",  # role label, e.g. "Комитет мүшесі" (committee member)
    "committee_role",
    "party_role",
    "dep_group_role",
    "is_active",  # always true in the current-convocation feed
    "committee",
    "avatar_url",
    "timestamp",
    "public_financial_disclosure",
]


def crawl_deputy(
    context: Context,
    row: dict[str, Any],
    ru: dict[str, Any] | None,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    person = context.make("Person")
    person.id = context.make_slug("deputy", str(row.pop("id")))
    # Default names are Kazakh Cyrillic; the Russian-Cyrillic forms differ in
    # transliteration (e.g. Қарақат / Каракат) and are kept as aliases.
    h.apply_name(
        person,
        first_name=row.pop("first_name"),
        patronymic=row.pop("patronymic"),
        last_name=row.pop("last_name"),
        lang="kaz",
    )
    if ru is not None:
        h.apply_name(
            person,
            first_name=ru.get("first_name"),
            patronymic=ru.get("patronymic"),
            last_name=ru.get("last_name"),
            lang="rus",
            alias=True,
        )
    email = row.pop("email")
    if email is not None:
        # A few source emails carry a stray space before the "@".
        person.add("email", email.replace(" ", ""))
    # Deputies of the Mäjilis must be citizens of Kazakhstan (Constitution art. 51(4)).
    # https://www.constituteproject.org/constitution/Kazakhstan_2017
    person.add("citizenship", "kz")

    region = row.pop("region")
    party = row.pop("party")
    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    # Single-mandate deputies carry an electoral region; party-list deputies do not.
    if region is not None:
        occupancy.add("constituency", region.get("name"), lang="kaz")
        if ru is not None and ru.get("region") is not None:
            occupancy.add("constituency", ru["region"].get("name"), lang="rus")
    if party is not None:
        occupancy.add("politicalGroup", party.get("name"), lang="kaz")
        if ru is not None and ru.get("party") is not None:
            occupancy.add("politicalGroup", ru["party"].get("name"), lang="rus")

    context.emit(occupancy)
    context.emit(person)
    context.audit_data(row, ignore=IGNORE)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Mäjilis of Kazakhstan",
        country="kz",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21328574",
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    deputies = context.fetch_json(context.data_url, params=PARAMS, cache_days=1)
    # The Russian variant lives at the same URL behind Accept-Language; zavod's cache
    # keys on URL only, so fetch it uncached to avoid colliding with the Kazakh response.
    ru_rows = context.fetch_json(
        context.data_url, params=PARAMS, headers={"Accept-Language": "ru"}
    )
    ru = {r["id"]: r for r in ru_rows}

    for row in deputies:
        crawl_deputy(context, row, ru.get(row["id"]), position, categorisation)
