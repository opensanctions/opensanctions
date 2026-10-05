import json
from rigour.mime.types import JSON

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise


def crawl(context: Context) -> None:
    path = context.fetch_resource("source.json", context.data_url)
    context.export_resource(path, JSON, title=context.SOURCE_TITLE)
    with open(path) as fh:
        rows: list[dict[str, str | None]] = json.load(fh)
        for row in rows:
            person = context.make("Person")
            person.id = context.make_slug(row.pop("ID"))
            h.apply_name(
                person, first_name=row.pop("NOMBRE"), last_name=row.pop("APELLIDO")
            )
            person.add("citizenship", "ar")
            person.add("gender", row.pop("GENERO"))
            person.add("political", row.pop("BLOQUE"))

            position = h.make_position(
                context,
                name="Member of the Argentine Chamber of Deputies",
                wikidata_id="Q18229570",
                country="ar",
                topics=["gov.legislative", "gov.national"],
                lang="eng",
            )
            categorisation = categorise(context, position, default_is_pep=True)

            occupancy = h.make_occupancy(
                context,
                person,
                position,
                False,
                start_date=row.pop("INICIO"),
                # CESE — actual end date; in normal cases equals FIN,
                # but would differ if the deputy resigned, died, or was removed mid-term
                end_date=row.pop("CESE"),
                categorisation=categorisation,
            )
            if occupancy is not None:
                context.emit(occupancy)
                context.emit(position)
                context.emit(person)

            context.audit_data(
                row,
                ignore=["JURAMENTO", "BLOQUE_INICIO", "BLOQUE_FIN", "FIN", "DISTRITO"],
            )
