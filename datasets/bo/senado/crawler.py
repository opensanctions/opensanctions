from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise


def crawl_senator(
    context: Context,
    row: dict[str, Any],
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    first_name = row.pop("nombre")
    last_name = row.pop("apellidos")
    department = context.lookup_value("brigada_department", row.pop("brigada"))

    person = context.make("Person")
    person.id = context.make_id(first_name, last_name, department)
    h.apply_name(person, first_name=first_name, last_name=last_name)
    h.apply_date(person, "birthDate", row.pop("fecha_nacimiento"))
    person.add("birthPlace", row.pop("lugar_nacimiento"))

    # Senators must be Bolivian citizens: citizenship is reserved to Bolivians and
    # includes the right to hold public office (Constitution arts. 144, 149-150).
    # https://pdba.georgetown.edu/Constitutions/Bolivia/bolivia09.html
    person.add("citizenship", "bo")
    person.add("political", row["bancada"]["nombre"])

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    occupancy.add("constituency", department)

    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Chamber of Senators of Bolivia",
        country="bo",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q20081427",
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    response = context.fetch_json(context.data_url)
    for row in response["data"]:
        # Skip alternates (suplentes) and previous-term rows
        if row.get("es_titular") != 83 or row.get("statusCode") != "A":
            continue
        crawl_senator(context, row, position, categorisation)
