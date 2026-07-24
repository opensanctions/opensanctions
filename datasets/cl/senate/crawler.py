from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise

GENDERS = {"Hombre": "male", "Mujer": "female"}


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Senator of Chile",
        country="cl",
        wikidata_id="Q18882653",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    payload = context.fetch_json(context.data_url, cache_days=1)
    senators = payload["data"]["parlamentarios"]["data"]
    if not senators:
        raise ValueError("Senate API returned no senators")

    for senator in senators:
        person = context.make("Person")
        person.id = context.make_slug(str(senator["ID_PARLAMENTARIO"]))
        person.add("name", senator.get("NOMBRE_COMPLETO"), lang="spa")
        person.add("gender", GENDERS.get(senator.get("SEXO_ETIQUETA")))
        person.add("political", senator.get("PARTIDO"), lang="spa")
        # Senators must be citizens with the right to vote (Constitution of Chile,
        # Article 50). https://www.constituteproject.org/constitution/Chile_2021
        person.add("citizenship", "cl")

        occupancy = h.make_occupancy(
            context, person, position, categorisation=categorisation
        )
        if occupancy is None:
            continue
        occupancy.add("constituency", senator.get("REGION"), lang="spa")
        context.emit(occupancy)
        context.emit(person)
