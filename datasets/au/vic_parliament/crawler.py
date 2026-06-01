from itertools import count
from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.extract.zyte_api import fetch_json
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

POSITIONS: dict[str, dict[str, Any]] = {
    "10": {
        "name": "Member of the Victorian Legislative Assembly",
        "wikidata_id": "Q18534408",
    },
    "20": {
        "name": "Member of the Victorian Legislative Council",
        "wikidata_id": "Q19185341",
    },
}


def crawl_member(
    context: Context,
    house_positions: dict[str, tuple[Entity, PositionCategorisation]],
    record: dict[str, Any],
) -> None:
    house = record.pop("house")
    if house not in house_positions:
        context.log.warning("Unknown house code", house=house)
        return
    position, categorisation = house_positions[house]

    person = context.make("Person")
    member_id = record.pop("id")
    person.id = context.make_slug("member", member_id)
    person_name = record.pop("title")
    person.add("name", person_name)
    person.add("nationality", "au")
    url = record.pop("url", None)
    if url is not None:
        person.add("sourceUrl", "https://www.parliament.vic.gov.au" + url)

    for membership in record.pop("memberships", []):
        if membership["title"] == "Party":
            for detail in membership["details"]:
                person.add("political", detail)

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        propagate_country=True,
    )
    if occupancy is not None:
        context.emit(occupancy)
        context.emit(person)

    context.audit_data(record, ignore=["subtitle", "image", "theme"])


def crawl(context: Context) -> None:
    house_positions: dict[str, tuple[Entity, PositionCategorisation]] = {}
    for house_id, config in POSITIONS.items():
        position = h.make_position(
            context,
            name=config["name"],
            country="au",
            wikidata_id=config["wikidata_id"],
        )
        categorisation = categorise(context, position, default_is_pep=True)
        context.emit(position)
        house_positions[house_id] = (position, categorisation)

    for page in count(1):
        url = f"{context.data_url}&page={page}"
        data = fetch_json(context, url, geolocation="au")
        result = data["result"]
        hits: list[dict[str, Any]] = result.get("hits", [])
        total: int = result.get("totalMatching", 0)

        for record in hits:
            crawl_member(context, house_positions, record)

        if not hits or page * 100 >= total:
            break
