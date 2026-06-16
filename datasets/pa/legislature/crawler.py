from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# Fields carried by the source we deliberately do not emit: contact details and social
# media (private), images and document links, attendance/score/budget metrics, and
# editorial profile text. The alternate deputy (suplente) is a different person who is
# not currently in office, so they are not emitted as a deputy either.
IGNORE_FIELDS = [
    "slug",
    "circuito",
    "provincia",
    "suplente",
    "imagen",
    "biografia",
    "email",
    "telefono",
    "redesSociales",
    "profesion",
    "educacion",
    "experiencia",
    "cv",
    "propuestaPolitica",
    "asistenciaPlenarias",
    "asistenciaComisiones",
    "proyectosPresentados",
    "proyectosAprobados",
    "intervenciones",
    "puntajeViajesViaticos",
    "puntajeDeclaracionIntereses",
    "puntajeDeclaracionPatrimonio",
    "calificacionPonderada",
    "planillaMonto",
    "viajesViaticosMonto",
    "comisionPrincipal",
    "comisionesSecundarias",
    "periodoInicio",
    "periodoFin",
    "declaracionIntereses",
    "declaracionPatrimonio",
    "tieneDeclaraciones",
    "planilla",
    "viajesViaticos",
    "createdAt",
    "updatedAt",
]


def crawl_deputy(
    context: Context,
    row: dict[str, Any],
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    # `activo` marks deputies currently in office; skip anyone flagged inactive.
    activo = row.pop("activo")
    if activo is not True:
        return

    deputy_id = row.pop("id")
    name = row.pop("nombreCompleto")
    row.pop("nombre")
    row.pop("apellido")
    born = row.pop("borndate")

    person = context.make("Person")
    person.id = context.make_slug(str(deputy_id))
    person.add("name", name)
    # `borndate` is an ISO timestamp (e.g. "1989-10-14T05:00:00.000Z"); keep the date.
    if born is not None:
        h.apply_date(person, "birthDate", born[:10])
    # Deputies must be Panamanian — by birth, or naturalised with fifteen years' residence
    # (Political Constitution of Panama, Art. 153).
    # https://constitucion.te.gob.pa/organo-legislativo/
    person.add("citizenship", "pa")
    person.add("political", row.pop("partido"))

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        no_end_implies_current=True,
        categorisation=categorisation,
    )
    if occupancy is None:
        return

    context.emit(occupancy)
    context.emit(person)
    context.audit_data(row, ignore=IGNORE_FIELDS)


def crawl(context: Context) -> None:
    data = context.fetch_json(context.data_url, cache_days=1)
    if data.get("success") is not True:
        raise ValueError("Unexpected API response: %r" % data.get("success"))
    deputies = data["diputados"]
    if len(deputies) < 50:
        raise ValueError("Expected at least 50 deputies, got %d" % len(deputies))

    position = h.make_position(
        context,
        name="Member of the National Assembly of Panama",
        country="pa",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21295996",
        lang="eng",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    context.emit(position)

    for row in deputies:
        crawl_deputy(context, row, position, categorisation)
