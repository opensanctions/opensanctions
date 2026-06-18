from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# The current legislative term. The open-data endpoint URL is stable across terms,
# so we assert the period on every record to fail loudly when the next term lands
# (and the source starts returning 2028-2033 senators).
EXPECTED_PERIOD = "2023-2028"

IGNORE_FIELDS = [
    "camaraParlamentario",  # constant "CAMARA DE SENADORES"
    "telefonoParlamentario",  # private contact detail
    "fotoURL",
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

    # The endpoint returns one record per seat — the senator currently occupying it.
    # tipoParlamentario flags whether that occupant is the elected titular or a
    # suplente who has taken over a vacated seat; both are sitting senators, so we
    # emit either. (We don't filter to TITULAR, which would drop seated replacements.)
    person = context.make("Person")
    person.id = context.make_slug(str(row.pop("idParlamentario")))
    h.apply_name(
        person, first_name=row.pop("nombres"), last_name=row.pop("apellidos")
    )
    # Senators must hold natural Paraguayan nationality; naturalised citizens are not
    # eligible (Constitution of Paraguay, Art. 223).
    # https://www.constituteproject.org/constitution/Paraguay_2011?lang=es
    person.add("citizenship", "py")
    person.add("political", row.pop("partidoPolitico"))
    person.add("email", row.pop("emailParlamentario"))
    person.add("sourceUrl", row.pop("appURL"))

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    # All senators are elected from a single nationwide constituency ("NACIONAL").
    occupancy.add("constituency", row.pop("departamento"))
    # The parliamentary bloc (bancada) is distinct from party membership.
    occupancy.add("politicalGroup", row.pop("bancada"))

    context.emit(occupancy)
    context.emit(person)
    context.audit_data(
        row,
        ignore=[
            "tipoParlamentario",  # titular vs seated suplente — both emitted
            "cargoBancada",  # role within the parliamentary bloc
            *IGNORE_FIELDS,
        ],
    )


def crawl(context: Context) -> None:
    senators = context.fetch_json(context.data_url)
    if not isinstance(senators, list) or len(senators) == 0:
        raise ValueError("Expected a non-empty list of senators")

    position = h.make_position(
        context,
        name="Member of the Chamber of Senators of Paraguay",
        country="py",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q20058559",
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    for row in senators:
        crawl_member(context, row, position, categorisation)
