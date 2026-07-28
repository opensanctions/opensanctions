from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Senator of Brazil",
        country="br",
        wikidata_id="Q18964326",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    data = context.fetch_json(context.data_url, cache_days=7)
    senators = data["ListaParlamentarEmExercicio"]["Parlamentares"]["Parlamentar"]

    for senator in senators:
        info = senator["IdentificacaoParlamentar"]
        person = context.make("Person")
        person.id = context.make_slug(info["CodigoParlamentar"])
        person.add("name", info.pop("NomeParlamentar"), lang="por")
        person.add("name", info.pop("NomeCompletoParlamentar"), lang="por")
        person.add("gender", info.pop("SexoParlamentar"))
        person.add("email", info.pop("EmailParlamentar"))
        person.add("political", info.pop("SiglaPartidoParlamentar"), lang="por")
        person.add("sourceUrl", info.pop("UrlPaginaParlamentar"))
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
