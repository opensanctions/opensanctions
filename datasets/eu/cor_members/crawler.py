from typing import Any, Dict

from zavod import Context
from zavod import helpers as h
from zavod.helpers.positions import OccupancyStatus

FORMATS = ("%d/%m/%Y",)

STATUS = {
    "Active": OccupancyStatus.CURRENT,
    "Incoming": OccupancyStatus.UNKNOWN,
}


def crawl_person(context: Context, item: Dict[str, Any]) -> None:
    person_id = item.pop("id")
    person = context.make("Person")
    person.id = context.make_slug(person_id)
    url = f"https://memberspage.cor.europa.eu/members/{person_id}"
    person.add("sourceUrl", url)
    h.apply_name(
        person,
        first_name=item.pop("firstName"),
        last_name=item.pop("lastName"),
    )

    country: Dict[str, str] = item.pop("country", {})
    person.add("country", country.pop("value"))

    function: Dict[str, str] = item.pop("memberFunction")
    function_name = function.pop("value")
    position_name = f"{function_name} of the European Committee of the Regions"
    position = h.make_position(context, name=position_name, country="eu")
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        no_end_implies_current=True,
        start_date=item.pop("from"),
        end_date=item.pop("to"),
    )
    if occupancy is not None:
        for value in item.pop("memberStatuses", []):
            status = STATUS[value]
            occupancy.add("status", status)
        context.emit(position)
        context.emit(occupancy)

    for body in item.pop("bodies", []):
        body_type = body.pop("type")
        body_name = body.pop("name")
        if body_type == "PoliticalGroup":
            person.add("political", body_name)
        elif body_type == "Body":
            person.add("position", body_name)

    context.audit_data(item)
    context.emit(person, target=True)


def crawl(context: Context):
    data = context.fetch_json(context.data_url)

    for item in data:
        crawl_person(context, item)
