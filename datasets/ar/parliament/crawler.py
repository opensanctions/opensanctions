import json
from rigour.mime.types import JSON

from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.json", context.data_url)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path, "r") as fh:
        data = json.load(fh)
    for entry in data:
        deputy_id = entry.pop("id")
        last_name = entry.pop("apellido")
        first_name = entry.pop("nombre")

        person = context.make("Person")
        person.id = context.make_id(first_name, last_name, deputy_id)
        h.apply_name(person, first_name=first_name, last_name=last_name)
        person.add("citizenship", "ar")
        person.add("gender", entry.pop("genero"))
        person.add("political", entry.pop("bloque"))

        position = h.make_position(
            context,
            name="Member of the Argentine Chamber of Deputies",
            wikidata_id="Q18229570",
            country="ar",
            topics=["gov.legislative", "gov.national"],
            lang="eng",
        )
        categorisation = categorise(context, position, is_pep=True)

        occupancy = h.make_occupancy(
            context,
            person,
            position,
            False,  # every tenure should have an end date (even if it is in the future)
            start_date=entry.pop("inicio"),
            end_date=entry.pop("fin"),
            categorisation=categorisation,
        )
        if occupancy is not None:
            context.emit(occupancy)
            context.emit(position)
            context.emit(person)

            context.audit_data(
                entry, ["distrito", "juramento", "cese", "bloque_inicio", "bloque_fin"]
            )
