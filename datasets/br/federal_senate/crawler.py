from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise

GENDERS = {"Masculino": "male", "Feminino": "female"}


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Senator of Brazil",
        country="br",
        wikidata_id="Q18964326",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    # The endpoint serves XML by default; request JSON explicitly.
    data = context.fetch_json(
        context.data_url, headers={"Accept": "application/json"}, cache_days=1
    )
    senators = data["ListaParlamentarEmExercicio"]["Parlamentares"]["Parlamentar"]
    if not senators:
        raise ValueError("Senate API returned no senators")

    for senator in senators:
        info = senator["IdentificacaoParlamentar"]
        person = context.make("Person")
        person.id = context.make_slug(info["CodigoParlamentar"])
        person.add("name", info.get("NomeParlamentar"), lang="por")
        person.add("name", info.get("NomeCompletoParlamentar"), lang="por")
        person.add("gender", GENDERS.get(info.get("SexoParlamentar")))
        person.add("political", info.get("SiglaPartidoParlamentar"), lang="por")
        person.add("sourceUrl", info.get("UrlPaginaParlamentar"))
        # Senators must be Brazilian nationals (Constitution of Brazil 1988,
        # Article 14 §3 I). https://www.constituteproject.org/constitution/Brazil_2017
        person.add("citizenship", "br")

        occupancy = h.make_occupancy(
            context, person, position, categorisation=categorisation
        )
        if occupancy is None:
            continue
        if info.get("UfParlamentar"):
            occupancy.add("constituency", info["UfParlamentar"])
        context.emit(occupancy)
        context.emit(person)
