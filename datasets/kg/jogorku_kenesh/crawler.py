from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

IGNORE = [
    "status",
    "createdBy",
    "imageUrl",
    "phone",  # private contact detail
    "facebook",
    "youtube",
    "instagram",
    "twitter",
    "telegram",
    "slug",
    "committees",  # committee memberships — no entity field
    "deputyCommittee",
    # Leadership role flags (chair, faction leader, etc.) — not modelled separately.
    "chairman",
    "firstDeputyChairman",
    "deputyChairman",
    "fractionLeader",
    "fractionLeaderAssistant",
    "headParliamentaryGroup",
    "deputyHeadParliamentaryGroup",
]


def crawl_deputy(
    context: Context,
    row: dict[str, Any],
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    person = context.make("Person")
    person.id = context.make_slug("deputy", str(row.pop("id")))
    # The API exposes a single (Russian Cyrillic) name form; patronymic is in `surname`.
    h.apply_name(
        person,
        first_name=row.pop("firstName"),
        patronymic=row.pop("surname"),
        last_name=row.pop("lastName"),
        lang="rus",
    )
    gender = row.pop("gender")
    if gender is not None:
        person.add("gender", gender.lower())
    person.add("email", row.pop("email"))
    # Deputies of the Jogorku Kenesh must be citizens of the Kyrgyz Republic
    # (Constitution art. 70). https://www.constituteproject.org/constitution/Kyrgyz_Republic_2016
    person.add("citizenship", "kg")

    factions = row.pop("factions")
    # The two constituency fields hold the Kyrgyz and Russian forms (the Ru/Kg suffixes
    # are mislabelled at the source); keep both.
    constituencies = [row.pop("constituencyRu"), row.pop("constituencyKg")]
    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    for value in constituencies:
        if value is not None:
            occupancy.add("constituency", value.strip())
    for faction in factions:
        occupancy.add("politicalGroup", faction.get("titleRu"), lang="rus")
        occupancy.add("politicalGroup", faction.get("titleKg"), lang="kir")

    context.emit(occupancy)
    context.emit(person)
    context.audit_data(row, ignore=IGNORE)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Jogorku Kenesh of the Kyrgyz Republic",
        country="kg",
        topics=["gov.national", "gov.legislative"],
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    data = context.fetch_json(context.data_url, cache_days=1)
    for row in data["content"]:
        crawl_deputy(context, row, position, categorisation)
