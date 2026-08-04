from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    senator: dict[str, Any],
) -> None:
    person = context.make("Person")
    person.id = context.make_slug(str(senator["ID_PARLAMENTARIO"]))
    first = senator.pop("NOMBRE")
    last = " ".join(
        p
        for p in (senator.pop("APELLIDO_PATERNO"), senator.pop("APELLIDO_MATERNO"))
        if p
    )
    h.apply_name(person, first_name=first, last_name=last, lang="spa")
    person.add("gender", senator.pop("SEXO_ETIQUETA"))
    person.add("political", senator.pop("PARTIDO"), lang="spa")
    person.add("email", senator.pop("EMAIL"))
    # Senators must be citizens with the right to vote (Constitution of Chile,
    # Article 50). https://www.constituteproject.org/constitution/Chile_2021
    person.add("citizenship", "cl")
    # The source gives one region per senator, not one per term.
    region = senator.pop("REGION", None)

    for period in senator.get("PERIODOS") or []:
        # A member's PERIODOS list covers their whole parliamentary career, so it also
        # carries their Chamber of Deputies terms (CAMARA="D"), which belong to a different
        # dataset. Only Senate terms are relevant here.
        if period.get("CAMARA") != "S":
            continue
        # PERIODOS gives term boundaries as years only.
        start, end = period.pop("DESDE"), period.pop("HASTA")
        if int(start) < int(h.earliest_term_start(position.get("topics"))[:4]):
            continue

        occupancy = h.make_occupancy(
            context,
            person,
            position,
            period_start=start,
            # The Chilean Congress is installed on 11 March, so the end year is anchored to
            # that date: a bare year only counts as elapsed once it is over, which would
            # leave a term ending in the current year (e.g. 2022-2026) indistinguishable
            # from an ongoing one. https://www.constituteproject.org/constitution/Chile_2021
            period_end=f"{end}-03-11",
            categorisation=categorisation,
        )
        if occupancy is None:
            continue
        occupancy.add("constituency", region, lang="spa")
        context.emit(occupancy)
        context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Senator of Chile",
        country="cl",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q18882653",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    payload = context.fetch_json(context.data_url, cache_days=14)
    senators = payload["data"]["parlamentarios"]["data"]
    for senator in senators:
        crawl_member(context, position, categorisation, senator)
