from typing import Any

from zavod import Context, helpers as h
from zavod.stateful.positions import categorise


POSITION_TOPICS = ["gov.national", "gov.legislative"]

# Maps the source's camaraParlamentario value to (position name, Wikidata QID).
CHAMBERS = {
    "CAMARA DE DIPUTADOS": (
        "Member of the Chamber of Deputies of Paraguay",
        "Q20058561",
    ),
    "CAMARA DE SENADORES": (
        "Member of the Chamber of Senators of Paraguay",
        "Q20058559",
    ),
}


def crawl_member(
    context: Context,
    row: dict[str, Any],
) -> None:
    period = row.pop("periodoLegislativo")
    period_start, period_end = period.split("-")

    if period_start < h.earliest_term_start(POSITION_TOPICS):
        return

    camara = row.pop("camaraParlamentario")
    if camara not in CHAMBERS:
        raise ValueError(f"Unexpected chamber value: {camara!r}")
    position_name, wikidata_id = CHAMBERS[camara]
    position = h.make_position(
        context,
        name=position_name,
        country="py",
        topics=POSITION_TOPICS,
        wikidata_id=wikidata_id,
        lang="eng",
    )
    categorisation = categorise(context, position)

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
    # Deputies are elected by department; senators from a single nationwide constituency ("NACIONAL")
    occupancy.add("constituency", row.pop("departamento"))
    occupancy.add("politicalGroup", row.pop("bancada"))  # caucus

    context.emit(position)
    context.emit(occupancy)
    context.emit(person)
    context.audit_data(
        row,
        ignore=[
            "telefonoParlamentario",  # private contact detail
            "fotoURL",
            "tipoParlamentario",  # titular vs seated suplente — both emitted
            "cargoBancada",  # role within the parliamentary bloc
        ],
    )


def crawl(context: Context) -> None:
    members = context.fetch_json(context.data_url)
    for row in members:
        crawl_member(context, row)
