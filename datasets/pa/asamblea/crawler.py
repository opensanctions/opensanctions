from typing import Any

from zavod import Context, helpers as h
from zavod.entity import Entity
from zavod.extract import zyte_api
from zavod.stateful.positions import PositionCategorisation, categorise


def crawl_deputy(
    context: Context,
    row: dict[str, Any],
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    persona = row.pop("Persona")
    name = persona.pop("Nombre_Completo")
    party = row.pop("Partido")

    person = context.make("Person")
    person.id = context.make_id(row.pop("Id_persona"), name)
    person.add("name", name)
    person.add("firstName", persona.pop("Nombres"))
    person.add("lastName", persona.pop("Apellidos"))
    person.add("political", party)
    person.add("email", persona.pop("Correo"))
    person.add("sourceUrl", f"https://www.asamblea.gob.pa/Diputados/{row.pop('Slug')}")

    # Deputies must be Panamanian — by birth, or naturalised with fifteen years'
    # residence (Political Constitution of Panama, Art. 153).
    # https://constitucion.te.gob.pa/organo-legislativo/
    person.add("citizenship", "pa")

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    occupancy.add("constituency", row.pop("Circuito"))
    occupancy.add("constituency", row.pop("Provincia"))

    context.audit_data(persona, ignore=["ID", "Diputado"])
    context.audit_data(row, ignore=["ID", "Foto", "Suplentes", "Hoja_vida"])

    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the National Assembly of Panama",
        country="pa",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21295996",
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    data = zyte_api.fetch_json(
        context, context.data_url, geolocation="pa", cache_days=1
    )
    for row in data["data"]:
        crawl_deputy(context, row, position, categorisation)
