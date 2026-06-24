from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# The current legislative term. The open-data endpoints are stable across terms, so we
# assert the period on every record to fail loudly when the next term lands (and the
# source starts returning 2028-2033 members).
EXPECTED_PERIOD = "2023-2028"

IGNORE_FIELDS = [
    "camaraParlamentario",  # constant per chamber ("CAMARA DE DIPUTADOS"/"DE SENADORES")
    "telefonoParlamentario",  # private contact detail
    "fotoURL",
    "tipoParlamentario",  # titular vs seated suplente — both emitted
    "cargoBancada",  # role within the parliamentary bloc
]


def crawl_member(
    context: Context,
    row: dict[str, Any],
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    period = row.pop("periodoLegislativo")
    if period != EXPECTED_PERIOD:
        raise ValueError(f"Unexpected legislative period: {period!r}")
    # The legislative term bounds (e.g. "2023-2028"); recorded as the occupancy's legal
    # period. The source has no individual start/end date for seated suplentes.
    period_start, period_end = period.split("-")

    # The endpoint returns one record per seat — the member currently occupying it.
    # tipoParlamentario flags whether that occupant is the elected titular or a suplente
    # who has taken over a vacated seat; both are sitting members, so we emit either.
    # (We don't filter to TITULAR, which would drop seated replacements.)
    person = context.make("Person")
    person.id = context.make_slug(str(row.pop("idParlamentario")))
    h.apply_name(person, first_name=row.pop("nombres"), last_name=row.pop("apellidos"))
    # Members of Congress must hold natural Paraguayan nationality; naturalised citizens
    # are not eligible (Constitution of Paraguay, Art. 221 for deputies, Art. 223 for
    # senators). https://www.constituteproject.org/constitution/Paraguay_2011?lang=es
    person.add("citizenship", "py")
    person.add("political", row.pop("partidoPolitico"))
    person.add("email", row.pop("emailParlamentario"))
    person.add("sourceUrl", row.pop("appURL"))

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        period_start=period_start,
        period_end=period_end,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    # Deputies are elected by department (plus the Capital District, Asunción); senators
    # from a single nationwide constituency (departamento "NACIONAL").
    occupancy.add("constituency", row.pop("departamento"))
    # The parliamentary bloc (bancada) is distinct from party membership.
    occupancy.add("politicalGroup", row.pop("bancada"))

    context.emit(occupancy)
    context.emit(person)
    context.audit_data(row, ignore=IGNORE_FIELDS)


def crawl_chamber(
    context: Context, path: str, position_name: str, wikidata_id: str
) -> None:
    members = context.fetch_json(context.data_url + path)
    if not isinstance(members, list) or len(members) == 0:
        raise ValueError(f"Expected a non-empty list of members from {path!r}")

    position = h.make_position(
        context,
        name=position_name,
        country="py",
        topics=["gov.national", "gov.legislative"],
        wikidata_id=wikidata_id,
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    for row in members:
        crawl_member(context, row, position, categorisation)


def crawl(context: Context) -> None:
    # Both chambers come from the same Congress open-data API; data_url is the
    # `.../camara/` base. limit=200 is required for the 80-seat Chamber of Deputies
    # (the default page size of 50 would truncate it) and harmless for the Senate.
    crawl_chamber(
        context,
        "S?limit=200",
        "Member of the Chamber of Senators of Paraguay",
        "Q20058559",
    )
    crawl_chamber(
        context,
        "D?limit=200",
        "Member of the Chamber of Deputies of Paraguay",
        "Q20058561",
    )
