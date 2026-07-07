from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise


def crawl_deputy(
    context: Context,
    row: dict[str, Any],
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    person = context.make("Person")
    raw_id = row.pop("id")
    person.id = context.make_slug("deputy", str(raw_id))
    h.apply_name(
        person,
        first_name=row.pop("firstName"),
        patronymic=row.pop("surname"),
        last_name=row.pop("lastName"),
        lang="rus",
    )
    person.add("gender", row.pop("gender").lower())
    person.add("email", row.pop("email"))
    # Deputies of the Jogorku Kenesh must be citizens of the Kyrgyz Republic
    # (Constitution art. 70). https://www.constituteproject.org/constitution/Kyrgyz_Republic_2016
    person.add("citizenship", "kg")
    person.add("sourceUrl", f"https://kenesh.kg/deputies/{raw_id}")

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return

    for value in [row["constituencyRu"], row["constituencyKg"]]:
        # add constituency names in kg and ru
        occupancy.add("constituency", value.strip())

    # can list several factions
    factions = row.pop("factions")
    for faction in factions:
        for value in [faction["titleRu"], faction["titleKg"]]:
            # add faction names in kg and ru
            occupancy.add("politicalGroup", value.strip())

    context.emit(occupancy)
    context.emit(person)


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
